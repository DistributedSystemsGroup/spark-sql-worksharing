package org.apache.spark.sql.extensions.cost

import java.io.InvalidObjectException

import fr.eurecom.dsg.cost.{CostConstants, Estimate}
import fr.eurecom.dsg.statistics.{ColumnStatistics, StatisticsProvider}
import fr.eurecom.dsg.util.SparkSQLServerLogging
import org.apache.spark.sql.catalyst.expressions.{Expression, _}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.extensions.Util
import org.apache.spark.sql.sources.BaseRelation


object CostEstimator extends SparkSQLServerLogging {
  var statsProvider: StatisticsProvider = null

  def setStatsProvider(provider: StatisticsProvider): Unit = {
    statsProvider = provider
  }

  /**
    * return the first match
    *
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

  private def estimateSelectivity(predicate:Expression, relationNames:Array[String]): Double ={
    var selectivity = CostConstants.DEFAULT_SELECTIVITY

    predicate match{
      // sel(a AND b) = sel(a) * sel(b)
      case and:And => selectivity = estimateSelectivity(and.left, relationNames) * estimateSelectivity(and.right, relationNames)

      // sel (a OR b) = sel(a) + sel(b) - sel(a) * sel(b)
      // OR equivalent to
      // 1 - [(1-sel(a)) * (1-sel(b))]
      case or:Or =>
        val leftSel = estimateSelectivity(or.left, relationNames)
        val rightSel = estimateSelectivity(or.right, relationNames)
        selectivity = leftSel + rightSel - leftSel*rightSel

      case eq:EqualTo =>
        (eq.left, eq.right) match{

          // colA = colB
          // if colA is unique || colB is unique
          // then numOutput = max(colA output, colB output)
          // else, hard to say
          case (colA:AttributeReference, colB:AttributeReference) =>
            val columnAStat = tryGetColumnStat(relationNames, colA.name)
            val columnBStat = tryGetColumnStat(relationNames, colB.name)
            (columnAStat, columnBStat) match {
              case (Some(colAStat), Some(colBStat)) =>
                if (colAStat.getUniquenessDegree() >= 0.9f)
                  selectivity = colBStat.numNotNull * 1.0 / (colAStat.numNotNull + colBStat.numNotNull)
                else if (colBStat.getUniquenessDegree() >= 0.9f)
                  selectivity = colAStat.numNotNull * 1.0 / (colAStat.numNotNull + colBStat.numNotNull)
                else if (colAStat.getUniquenessDegree() >= 0.9f && colBStat.getUniquenessDegree() >= 0.9f)
                  selectivity = Math.max(colAStat.numNotNull, colBStat.numNotNull) * 1.0 / (colAStat.numNotNull + colBStat.numNotNull)

              case _ =>
            }

          // col = c
          case (col:AttributeReference, c:Literal) =>
            val columnStat = tryGetColumnStat(relationNames, col.name)
            columnStat match {
              case Some(columnStat) => selectivity = columnStat.getEqualityEstimation(c.value)
              case None =>
            }

          // c = col
          case (c:Literal, col:AttributeReference) =>
            val columnStat = tryGetColumnStat(relationNames, col.name)
            columnStat match {
              case Some(columnStat) => selectivity = columnStat.getEqualityEstimation(c.value)
              case None =>
            }
          case _ => throw new NotImplementedError(eq.left.toString() + " EqualTo " + eq.right.toString())
        }

      // col Like c
      case l:Like =>
        (l.left, l.right) match {
          case (col:AttributeReference, c:Literal) =>
            val columnStat = tryGetColumnStat(relationNames, col.name)
            columnStat match {
              case Some(colStat) => selectivity = colStat.getEqualityEstimation(c.value)
              case None =>
            }

          case _ => throw new NotImplementedError(l.left.toString() + " Like " + l.right.toString())
        }

      // sel(col > C)
      case gt:GreaterThan =>
        (gt.left, gt.right) match{
          // col > c
          case (col:AttributeReference, c:Literal) =>
            val columnStat = tryGetColumnStat(relationNames, col.name)
            columnStat match {
              case Some(columnStat) => selectivity = columnStat.getGreaterThanEstimation(c.value)
              case None =>
            }
          case _ => throw new NotImplementedError(gt.left.toString() + " GreaterThan " + gt.right.toString())
        }

      // sel(col < c)
      case lt:LessThan =>
        (lt.left, lt.right) match{
          // col < c
          case (col:AttributeReference, c:Literal) =>
            val columnStat = tryGetColumnStat(relationNames, col.name)
            columnStat match {
              case Some(columnStat) => selectivity = columnStat.getLessThanEstimation(c.value)
              case None =>
            }
          case _ => throw new NotImplementedError(lt.left.toString() + " LessThan " + lt.right.toString())
        }

      // sel(col >= c)
      case gteq:GreaterThanOrEqual =>
        (gteq.left, gteq.right) match{
          // col < c
          case (col:AttributeReference, c:Literal) =>
            val columnStat = tryGetColumnStat(relationNames, col.name)
            columnStat match {
              case Some(columnStat) => selectivity = columnStat.getEqualityEstimation(c.value) + columnStat.getGreaterThanEstimation(c.value)
              case None =>
            }
          case _ => throw new NotImplementedError(gteq.left.toString() + " GreaterThanOrEqual " + gteq.right.toString())
        }

      // sel(col <= c)
      case lteq:LessThanOrEqual =>
        (lteq.left, lteq.right) match{
          // col < c
          case (col:AttributeReference, c:Literal) =>
            val columnStat = tryGetColumnStat(relationNames, col.name)
            columnStat match {
              case Some(columnStat) => selectivity = columnStat.getEqualityEstimation(c.value) + columnStat.getLessThanEstimation(c.value)
              case None =>
            }
          case _ => throw new NotImplementedError(lteq.left.toString() + " GreaterThanOrEqual " + lteq.right.toString())
        }

      case notNul:IsNotNull =>
        notNul.child match {
          case a:AttributeReference =>
            val columnStat = tryGetColumnStat(relationNames, a.name)
            columnStat match {
              case Some(c) => selectivity = c.numNotNull*1.0 / (c.numNull + c.numNotNull)
              case None =>
            }
          case _ => throw new NotImplementedError(predicate.getClass.toString)
        }


      case nul:IsNull =>
        throw new NotImplementedError(predicate.getClass.toString)

      case _ => throw new NotImplementedError(predicate.getClass.toString)
    }
    selectivity
  }

  private def estimateSelectivity(filter:Filter): Double = {
    val relationNames = Util.getLogicalRelations(filter).map(r => Util.extractTableName(r.relation)).toArray
    estimateSelectivity(filter.condition, relationNames)
  }

  /**
    * estimate the cost of executing a given logical plan
    * Normally, this should be declared in the class of each operator
    * However, we want the framework to be an extension, without modifying the core of Spark/ Spark SQL
    * So, we implement here
    *
    * @param plan logicalPlan (tree)
    * @return estimated cost of evaluating this plan
    */
  def estimateCost(plan: LogicalPlan): Estimate = {
    var estimatedResult:Estimate = null

    plan match {
      case l: LeafNode => l match {
        case l@LogicalRelation(baseRelation: BaseRelation, _, _) =>
          estimatedResult = new Estimate()
          val tableName = Util.extractTableName(baseRelation)
          val tableStats = statsProvider.getTableStatistics(tableName)

          estimatedResult.setNumRecInput(tableStats.numRecords)
          estimatedResult.setNumRecOutput(tableStats.numRecords)
          estimatedResult.setInputSize(tableStats.inputSize)
          estimatedResult.setOutputSize(tableStats.inputSize)
          // scan cost
          estimatedResult.setCPUCost(tableStats.numRecords * CostConstants.COST_DISK_READ)
          // no network cost?
        case _ => throw new NotImplementedError(l.toString())
      }

      case u: UnaryNode =>
        val childCost = estimateCost(u.child)
        u match {
          case p@Project(projectList, child) =>
            // Projection would potentially remove some column(s), therefore reduce the outputSize
            // add cheap cpu cost
            // Heuristic is applied: based on the number of fields projected
            // TODO: better to check the datatype of each field
            val numFieldsChild = child.output.length
            val numFieldsThis = p.output.length
            val fraction: Double = numFieldsThis * 1.0 / numFieldsChild

            estimatedResult = childCost

            estimatedResult.addCPUCost(estimatedResult.getNumRecOutput * CostConstants.COST_SIMPLE_OP)
            estimatedResult.setOutputSize((estimatedResult.getOutputSize * fraction).toLong)

          case f:Filter =>
            // Filtering would potentially filter out some row(s), therefore reduce the numRecOutput, outputSize
            // Compute the selectivity factor "sel" of this f
            // numRecOutput = childNumRecOutput * sel
            // OutputSize = childOutputSize * sel
            // add cheap cpu cost
            val selectivity = estimateSelectivity(f)

            estimatedResult = childCost

            estimatedResult.addCPUCost(estimatedResult.getNumRecOutput * CostConstants.COST_SIMPLE_OP)
            estimatedResult.setOutputSize((estimatedResult.getOutputSize * selectivity).toLong)
            estimatedResult.setNumRecOutput((estimatedResult.getNumRecOutput * selectivity).toLong)

          case d@Distinct(child) =>
            // potentially remove some row(s), therefore reduce the numRecOutput, outputSize
            // reduce by magic number, .7
            // add expensive cpu & network cost

            estimatedResult = childCost

            estimatedResult.addCPUCost(estimatedResult.getNumRecOutput * CostConstants.COST_SIMPLE_OP)
            estimatedResult.addNetworkCost(estimatedResult.getNumRecOutput * CostConstants.COST_NETWORK)

            estimatedResult.setOutputSize((estimatedResult.getOutputSize * 0.7f).toLong)
            estimatedResult.setNumRecOutput((estimatedResult.getNumRecOutput * 0.7f).toLong)

          case a@Aggregate(groupingExpressions, aggregateExpressions, child) =>
            // remove some columns
            // Heuristic is applied: based on the number of output fields
            // groupby => reduce numRecOutput. Magic number .7
            // add expensive cpu & network cost
            val numFieldsChild = child.output.length
            val numFieldsThis = a.output.length
            val fraction: Double = numFieldsThis * 1.0 / numFieldsChild
            estimatedResult = childCost

            estimatedResult.addCPUCost(estimatedResult.getNumRecOutput * 2*CostConstants.COST_SIMPLE_OP)
            estimatedResult.addNetworkCost(estimatedResult.getNumRecOutput * CostConstants.COST_NETWORK)

            estimatedResult.setOutputSize((estimatedResult.getOutputSize * fraction).toLong)
            estimatedResult.setOutputSize((estimatedResult.getOutputSize * 0.7f).toLong)
            estimatedResult.setNumRecOutput((estimatedResult.getNumRecOutput * 0.7f).toLong)

          case l@LocalLimit(limitExpr, child) =>
            // add cheap cpu cost
            // numRecOutput = n
            // where the expression is limitExpr(n)

            val nMaxOutputRows = l.maxRows.get
            val reducedF = nMaxOutputRows * 1.0 / childCost.getNumRecOutput

            estimatedResult = childCost

            estimatedResult.addCPUCost(estimatedResult.getNumRecOutput * CostConstants.COST_SIMPLE_OP)

            estimatedResult.setNumRecOutput(nMaxOutputRows)
            estimatedResult.setOutputSize((reducedF * estimatedResult.getOutputSize).toLong)

          case l@GlobalLimit(limitExpr, child) =>
            // add cheap cpu cost
            // so recout =n trong limit(n)
            val value = l.limitExpr.asInstanceOf[Literal].value.toString.toLong
            val reducedF = value * 1.0 / childCost.getNumRecOutput

            estimatedResult = childCost

            estimatedResult.addCPUCost(estimatedResult.getNumRecOutput * CostConstants.COST_SIMPLE_OP)
            estimatedResult.addNetworkCost(estimatedResult.getNumRecOutput * CostConstants.COST_NETWORK)

            estimatedResult.setNumRecOutput(value)
            estimatedResult.setOutputSize((reducedF * estimatedResult.getOutputSize).toLong)

          case s@Sort(order, global, child) =>
            // sorting cost, shuffling cost
            // add expensive cpu & network cost
            estimatedResult = childCost
            estimatedResult.addCPUCost(estimatedResult.getNumRecOutput *  Math.log10(estimatedResult.getNumRecOutput * CostConstants.COST_SIMPLE_OP))
            estimatedResult.addNetworkCost(estimatedResult.getNumRecOutput * CostConstants.COST_NETWORK)

          case _ => throw new NotImplementedError(u.toString)
        }

      case b: BinaryNode =>
        val costLeftChild = estimateCost(b.left)
        val costRightChild = estimateCost(b.right)

        estimatedResult = new Estimate()
        estimatedResult.add(costLeftChild)
        estimatedResult.add(costRightChild)

        // add cpu cost
        estimatedResult.addCPUCost(estimatedResult.getNumRecOutput * CostConstants.COST_SIMPLE_OP)

        // add network cost
        if (costLeftChild.getNumRecOutput < costRightChild.getNumRecOutput)
          estimatedResult.addNetworkCost(costLeftChild.getNumRecOutput * CostConstants.COST_NETWORK)
        else
          estimatedResult.addNetworkCost(costRightChild.getNumRecOutput * CostConstants.COST_NETWORK)

        b match {
          case j@Join(left, right, joinType, condition) =>
            // TODO: broadcast join vs shuffle join should be handled differently

            // add expensive cpu & network cost
            // tuple size might increase as well
            // #records might increase as well

            // if colA is unique || colB is unique
            // then numOutput = max(colA output, colB output)
            // else
            // x = A JOIN B (a=b)
            // |x| = (|A| * |B|) / max(card(a), card(b))

            condition.get match {
              // join a.1 = b.2
              case cond:EqualTo =>
                val relationNames = Util.getLogicalRelations(j).map(r => Util.extractTableName(r.relation)).toArray
                val colAName = cond.left.asInstanceOf[AttributeReference].name
                val colBName = cond.right.asInstanceOf[AttributeReference].name
                val columnStatA = tryGetColumnStat(relationNames, colAName)
                val columnStatB = tryGetColumnStat(relationNames, colBName)
                (columnStatA, columnStatB) match {
                  case (Some(a), Some(b)) =>
                    val leftCard = a.numDistincts
                    val rightCard = b.numDistincts
                    val nOutRec:Long = ((costLeftChild.getNumRecOutput * costRightChild.getNumRecOutput)*1.0 / Math.max(leftCard, rightCard)).toLong
                    val f = nOutRec*1.0 / estimatedResult.getNumRecOutput
                    estimatedResult.setNumRecOutput(nOutRec)
                    estimatedResult.setOutputSize((estimatedResult.getOutputSize * f).toLong)
                  case _ =>
                }

              case _ => throw new NotImplementedError(condition.get.toString)
            }

          case e@Except(left, right) => throw new NotImplementedError(e.toString)

          case i@Intersect(left, right) => throw new NotImplementedError(i.toString)

          case _ => throw new NotImplementedError(b.toString)
        }

      case _ => throw new NotImplementedError(plan.toString)
    }

    logInfo("Operator: %s - Cost: %s".format(plan.getClass.getSimpleName, estimatedResult))
    estimatedResult
  }


  def estimateMaterializingCost(amount:Long):Double = {
    amount * CostConstants.COST_RAM_WRITE
  }

  def estimateRetrievingCost(amount:Long):Double = {
    amount * CostConstants.COST_RAM_WRITE
  }



}
