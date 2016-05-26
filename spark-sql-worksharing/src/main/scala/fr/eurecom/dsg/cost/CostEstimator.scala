package org.apache.spark.sql.myExtensions.cost

import java.io.InvalidObjectException
import fr.eurecom.dsg.cost.{CostConstants, Estimate}
import fr.eurecom.dsg.util.{SparkSQLServerLogging, Constants}
import fr.eurecom.dsg.statistics.{ColumnStatistics, StatisticsProvider}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.extensions.Util


object CostEstimator extends SparkSQLServerLogging {
  var statsProvider: StatisticsProvider = null

  def setStatsProvider(provider: StatisticsProvider): Unit = {
    statsProvider = provider
  }

  /**
    * return the first match
    * @param relations
    * @param columnName
    * @return null, or first match
    */
  private def tryGetColumnStat(relations: Array[String], columnName: String): Option[ColumnStatistics] = {
    relations.indices.foreach(i =>{
      val tableStat = statsProvider.getTableStatistics(relations(i))
      val columnStat = tableStat.getColumnStats(columnName)
      if (columnStat != null)
        return Some(columnStat)
    })
    None
  }


  private def estimateSelectivity(filter:Filter):Double ={
    var selectivity = CostConstants.DEFAULT_SELECTIVITY
    val predicate = filter.condition
    val child = filter.child



    selectivity
  }



  private def estimateSelectivity(plan: LogicalPlan, predicate: Expression): Double = {
    var res = CostConstants.DEFAULT_SELECTIVITY
    val relations = plan.collect { case r: LogicalRelation => r }
    val relationNames = relations.map(r => Util.extractTableName(r.relation)).toArray

    /**
      *
      * @param expression
      * @return {UNKNOWN_VAL_DOUBLE, numRec}
      */
    def estimateExpression(expression: Expression): Double = {
      expression match {
        case e: GreaterThan => {
          val column = e.left.asInstanceOf[AttributeReference].name
          val value = e.right.asInstanceOf[Literal].value
          val columnStat = tryGetColumnStat(relationNames, column)
          columnStat match {
            case None => return Constants.UNKNOWN_VAL_DOUBLE
            case Some(c)=> return c.getGreaterThanEstimation(value)
          }
        }

        case e: LessThan => {
          val column = e.left.asInstanceOf[AttributeReference].name
          val value = e.right.asInstanceOf[Literal].value
          val columnStat = tryGetColumnStat(relationNames, column)
          columnStat match {
            case None => return Constants.UNKNOWN_VAL_DOUBLE
            case Some(c) => return c.getLessThanEstimation(value)
          }
        }

        case e: EqualTo => {
          e.right match {
            case value: Literal =>
              val column = e.left.asInstanceOf[AttributeReference].name
              val value = e.right.asInstanceOf[Literal].value
              val columnStat = tryGetColumnStat(relationNames, column)
              columnStat match {
                case None => return Constants.UNKNOWN_VAL_DOUBLE
                case Some(c) => return c.getEqualityEstimation(value)
              }
            case anotherColumn: AttributeReference =>
              // colA == colB
              // if colA is unique || colB is unique
              // then numOutput = max(colA output, colB output)

              // else, hard to say

              val columnA = e.left.asInstanceOf[AttributeReference].name
              val columnB = e.right.asInstanceOf[AttributeReference].name
              val columnStatA = tryGetColumnStat(relationNames, columnA)
              val columnStatB = tryGetColumnStat(relationNames, columnB)


              (columnStatA, columnStatB) match {
                case (None, None) | (_, None) | (None, _) => return Constants.UNKNOWN_VAL_DOUBLE
                case (Some(a), Some(b)) =>
                  if (a.getUniquenessDegree() >= 0.9f)
                    return b.numNotNull * 1.0 / (a.numNotNull + b.numNotNull)
                  else if (b.getUniquenessDegree() >= 0.9f)
                    return a.numNotNull * 1.0 / (a.numNotNull + b.numNotNull)
                  else if (a.getUniquenessDegree() >= 0.9f && b.getUniquenessDegree() >= 0.9f)
                    return Math.max(a.numNotNull, b.numNotNull) * 1.0 / (a.numNotNull + b.numNotNull)
                  else
                    return Constants.UNKNOWN_VAL_DOUBLE
              }
          }
        }

        case e: Like => {
          val column = e.left.asInstanceOf[AttributeReference].name
          val value = e.right.asInstanceOf[Literal].value
          val columnStat = tryGetColumnStat(relationNames, column)
          columnStat match {
            case None => return Constants.UNKNOWN_VAL_DOUBLE
            case Some(c) => return c.getEqualityEstimation(value)
          }
        }

        case e: IsNotNull => {

        }

        case e: IsNull =>

        case e: GreaterThanOrEqual => {
          val column = e.left.asInstanceOf[AttributeReference].name
          val value = e.right.asInstanceOf[Literal].value
          val columnStat = tryGetColumnStat(relationNames, column)
          columnStat match {
            case None => return Constants.UNKNOWN_VAL_DOUBLE
            case Some(c) => return c.getGreaterThanEstimation(value) + c.getEqualityEstimation(value)
          }
        }
        case e: LessThanOrEqual => {
          val column = e.left.asInstanceOf[AttributeReference].name
          val value = e.right.asInstanceOf[Literal].value
          val columnStat = tryGetColumnStat(relationNames, column)
          columnStat match {
            case None => return Constants.UNKNOWN_VAL_DOUBLE
            case Some(c) => return c.getLessThanEstimation(value) + c.getEqualityEstimation(value)
          }
        }

        case e: And =>
          // Heuristics
          // Selectivity(AND(a, b)) = Selectivity(a) * Selectivity(b)
          (estimateExpression(e.left), estimateExpression(e.right)) match {
            case (Constants.UNKNOWN_VAL_DOUBLE, _) | (_, Constants.UNKNOWN_VAL_DOUBLE) => return Constants.UNKNOWN_VAL_DOUBLE
            case (l, r) => return l * r
          }
        case e: Or =>
          // Heuristics
          // Selectivity(OR(a, b)) = 1 - [(1-Selectivity(a)) * (1-Selectivity(b))]
          (estimateExpression(e.left), estimateExpression(e.right)) match {
            case (Constants.UNKNOWN_VAL_DOUBLE, _) | (_, Constants.UNKNOWN_VAL_DOUBLE) => return Constants.UNKNOWN_VAL_DOUBLE
            case (l, r) => return 1 - (1 - l) * (1 - r)
          }
      }

      val res = estimateExpression(expression)
      if (res == Constants.UNKNOWN_VAL_DOUBLE)
        CostConstants.DEFAULT_SELECTIVITY
      else
        res
    }
    estimateExpression(predicate)
  }


  /**
    * estimate the cost of executing a given logical plan
    * Normally, this should be declared in the class of each operator
    * However, we want the framework to be an extension, without modifying the core of Spark/ Spark SQL
    * So, we implement here
    * @param plan logicalPlan (tree)
    * @return estimated cost of evaluating this plan
    */
  def estimateCost(plan: LogicalPlan): Estimate = {
    var estimateResult:Estimate = null

    plan match {
      case l: LeafNode => l match {
        case l@LogicalRelation(baseRelation: BaseRelation, _, _) =>
          estimateResult = new Estimate()
          val tableName = Util.extractTableName(baseRelation)
          val tableStats = statsProvider.getTableStatistics(tableName)
          estimateResult.addnumRecInput(tableStats.numRecords)
          estimateResult.addnumRecOutput(tableStats.numRecords)
          estimateResult.addInputSize(tableStats.inputSize)
          estimateResult.addOutputSize(tableStats.inputSize)
          estimateResult.addCPUCost(tableStats.numRecords * CostConstants.COST_DISK_READ)
      }
      case u: UnaryNode => {
        val cost_Child = estimateCost(u.child)
        u match {
          case p@Project(projectList, child) =>

            // Project(*)
            // Projection could remove some column(s), therefore reduce the "outputSize" and "average tuple size" too
            // add cheap cpu cost
            // Heuristic applied: based on the number of fields projected
            // TODO: better to check the datatype of each field
            val numFieldsChild = child.output.length
            val numFieldsThis = p.output.length
            val fraction: Double = numFieldsThis * 1.0 / numFieldsChild
            cost_Child.setOutputSize((cost_Child.getOutputSize * fraction).toLong)
            estimateResult = cost_Child

          case f@Filter(condition, child) =>
            // compute the selectivity factor S of this op
            // numRecOutput = childNumRecOutput * S
            // OutputSize = childOutputSize * S
            // add cheap cpu cost
            val selectivityFactor = estimateSelectivity(f, f.condition)
            cost_Child.setOutputSize((cost_Child.getOutputSize * selectivityFactor).toLong)
            cost_Child.setNumRecOutput((cost_Child.getNumRecOutput * selectivityFactor).toLong)
            estimateResult = cost_Child

          case d@Distinct(child) => new Estimate()
            // if it is distinct(relation) then numRecOut = max(card(relation))
            // else: reduce by magic number, 70%?
            // add expensive cpu & network cost
            estimateResult = cost_Child

          case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
            // nhu projection
            // bo 1 so cot
            // them 1 so cot
            // nhu plan
            // gom 1 so dong (giam # records) #distinct of each column
            // add expensive cpu & network cost


            estimateResult = cost_Child


          case l@Limit(limitExpr, child) => {
            // add cheap cpu cost
            // so recout =n trong limit(n)
            val value = l.limitExpr.asInstanceOf[Literal].value.toString.toLong
            val reducedF = value * 1.0 / cost_Child.getNumRecOutput()
            cost_Child.setNumRecOutput(value)
            cost_Child.setOutputSize((reducedF * cost_Child.getOutputSize).toLong)

            estimateResult = cost_Child
          }


          case s@Sort(order, global, child) => new Estimate()
            // add expensive cpu & network cost
            cost_Child.addCPUCost(cost_Child.getNumRecOutput() * Math.log10(cost_Child.getNumRecOutput()) * CostConstants.COST_CPU)
            estimateResult = cost_Child

          case _ => throw new InvalidObjectException(u.toString)
        }
      }

      case b: BinaryNode =>
        val costLeftChild = estimateCost(b.left)
        val costRightChild = estimateCost(b.right)
        val costSum = costLeftChild.add(costRightChild)
        costSum.setNumRecOutput(costLeftChild.getNumRecOutput * costRightChild.getNumRecOutput)
        costSum.setOutputSize(costLeftChild.getOutputSize * costRightChild.getOutputSize)

        b match {
          case j@Join(left, right, joinType, condition) => {
            // broadcast join vs shuffle join should be handled differently

            // more network cost
            // more CPU cost
            // tupple size might increase as well
            // #records might increase as well
            costSum.addCPUCost((costLeftChild.getNumRecOutput() + costRightChild.getNumRecOutput()) * CostConstants.COST_CPU)
            if (costLeftChild.getNumRecOutput() < costRightChild.getNumRecOutput())
              costSum.addNetworkCost(costLeftChild.getNumRecOutput() * CostConstants.COST_NETWORK)
            else
              costSum.addNetworkCost(costRightChild.getNumRecOutput() * CostConstants.COST_NETWORK)

            var selectivity:Double = 0.5f
            condition match {
              case None =>
              case Some(e) =>
                val expression = e.asInstanceOf[EqualTo]
                val relations = plan.collect { case r: LogicalRelation => r }
                val relationNames = relations.map(r => Util.extractTableName(r.relation)).toArray
                // colA == colB
                // if colA is unique || colB is unique
                // then numOutput = max(colA output, colB output)

                // else, hard to say

                val columnA = expression.left.asInstanceOf[AttributeReference].name
                val columnB = expression.right.asInstanceOf[AttributeReference].name

                var estimateA:Estimate = null
                var estimateB:Estimate = null

                if(left.output.contains(expression.left)){
                  estimateA = costLeftChild
                  estimateB = costRightChild
                }
                else{
                  estimateA = costRightChild
                  estimateB = costLeftChild
                }

                val columnStatA = tryGetColumnStat(relationNames, columnA)
                val columnStatB = tryGetColumnStat(relationNames, columnB)


                (columnStatA, columnStatB) match {
                  case (None, None) | (_, None) | (None, _) =>
                  case (Some(a), Some(b)) =>
                    if (a.getUniquenessDegree() >= 0.9f)
                      selectivity = estimateB.getNumRecOutput()*1.0 / costSum.getNumRecOutput()
                    else if (b.getUniquenessDegree() >= 0.9f)
                      selectivity = estimateA.getNumRecOutput()*1.0 / costSum.getNumRecOutput()
                    else if (a.getUniquenessDegree() >= 0.9f && b.getUniquenessDegree() >= 0.9f)
                      selectivity = Math.max(estimateA.getNumRecOutput(), estimateB.getNumRecOutput())*1.0 / costSum.getNumRecOutput()
                }

            }
            costSum.setNumRecOutput((costSum.getNumRecOutput() * selectivity).toLong)
            costSum.setOutputSize((costSum.getOutputSize * selectivity).toLong)
            estimateResult = costSum
          }



          case e@Except(left, right) => estimateResult = costSum

          case i@Intersect(left, right) => estimateResult = costSum

          case _ => throw new InvalidObjectException(b.toString)
        }
      case u@Union(child) => {
        //TODO:
      }
      case _ => throw new InvalidObjectException(plan.toString)
    }

    logInfo("Operator: %s - Cost: %s".format(plan.getClass.getSimpleName, estimateResult))
    estimateResult
  }

}
