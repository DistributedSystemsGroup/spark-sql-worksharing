package org.apache.spark.sql.myExtensions.cost

import java.io.InvalidObjectException

import com.databricks.spark.csv.CsvRelation
import nw.fr.eurecom.dsg.cost.{CostConstants, Estimation}
import nw.fr.eurecom.dsg.statistics.StatisticsProvider
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.json.JSONRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.catalyst.expressions._



object CostEstimator {
  var statsProvider: StatisticsProvider = null

  def setStatsProvider(provider: StatisticsProvider): Unit = {
    statsProvider = provider
  }

  private def extractTableName(r: BaseRelation): String = {
    r match {
      case r: JSONRelation => val path = r.paths(0)
        path.substring(path.lastIndexOf('/') + 1, path.length)
      case r: ParquetRelation => val path = r.paths(0)
        path.substring(path.lastIndexOf('/') + 1, path.length)
      case r: CsvRelation => val path = r.location.get
        path.substring(path.lastIndexOf('/') + 1, path.length)
      case _ => throw new InvalidObjectException(r.toString)
    }
  }


  private def estimateSelectivity(filterOp:Filter):Double={
    val DEFAULT_SELECTIVITY_FACTOR = 0.33f
    var res = DEFAULT_SELECTIVITY_FACTOR
    val relations = filterOp.collect{case r:LogicalRelation => r}.toArray

    def estimateExpression(expression:Expression): Double ={
      expression match{
        case e:GreaterThan =>
        case e:LessThan =>
        case e:EqualTo =>
        case e:Like =>
        case e:IsNotNull =>
        case e:GreaterThanOrEqual =>
        case e:LessThanOrEqual =>

        case e:And =>
          // Heuristics
          // Selectivity(AND(a, b)) = Selectivity(a) * Selectivity(b)
          estimateExpression(e.left) * estimateExpression(e.right)
        case e:Or =>
          // Heuristics
          // Selectivity(OR(a, b)) = 1 - [(1-Selectivity(a)) * (1-Selectivity(b))]
          1 - ((1 - estimateExpression(e.left)) * (1 - estimateExpression(e.right)))
      }


      DEFAULT_SELECTIVITY_FACTOR
    }

    res
  }


  /**
    * estimate the cost of executing a given logical plan
    * Normally, this should be declared in the class of each operator
    * However, we want the framework to be an extension, without modifying the core of Spark/ Spark SQL
    * So, we implement here
    * @param plan logicalPlan (tree)
    * @return estimated cost of evaluating this plan
    */
  def estimateCost(plan: LogicalPlan): Estimation = {
    plan match {
      case l: LeafNode => l match {
        case l@LogicalRelation(baseRelation: BaseRelation, _) =>
          val est = new Estimation()
          val tableName = extractTableName(baseRelation)
          val tableStats = statsProvider.getTableStatistics(tableName)
          est.addnumRecInput(tableStats.numRecords)
          est.addnumRecOutput(tableStats.numRecords)
          est.addInputSize(tableStats.inputSize)
          est.addOutputSize(tableStats.inputSize)
          est.addCPUCost(tableStats.numRecords * CostConstants.COST_DISK_READ)
          est
      }
      case u: UnaryNode => {
        val cost_Child = estimateCost(u.child)
        u match {
          case p @ Project(projectList, child) =>

            // Project(*)
            // Projection could remove some column(s), therefore reduce the "outputSize" and "average tuple size" too
            // add cheap cpu cost
            // Heuristic applied: based on the number of fields projected
            // TODO: better to check the datatype of each field
            val numFieldsChild = child.output.length
            val numFieldsThis = p.output.length
            val fraction:Double = numFieldsThis*1.0/numFieldsChild
            cost_Child.setOutputSize((cost_Child.getOutputSize * fraction).toLong)
            cost_Child

          case f @ Filter(condition, child) =>
            // compute the selectivity factor S of this op
            // numRecOutput = childNumRecOutput * S
            // OutputSize = childOutputSize * S
            // add cheap cpu cost

            val selectivityFactor = estimateSelectivity(f)
            cost_Child.setOutputSize((cost_Child.getOutputSize * selectivityFactor).toLong)
            cost_Child.setNumRecOutput((cost_Child.getNumRecOutput * selectivityFactor).toLong)
            cost_Child
          case d @ Distinct(child) =>new Estimation()
            // if it is distinct(relation) then numRecOut = max(card(relation))
            // else: reduce by magic number, 70%?
            // add expensive cpu & network cost
            cost_Child

          case a @ Aggregate(groupingExpressions, aggregateExpressions, child) =>
            // nhu projection
            // bo 1 so cot
            // them 1 so cot
            // nhu filterOp
            // gom 1 so dong
            // add expensive cpu & network cost


            cost_Child


          case l @ Limit(limitExpr, child) =>new Estimation()
            // add cheap cpu cost
            // so recout =n trong limit(n)
            cost_Child

          case s @ Sort(order, global, child) =>new Estimation()
            // add expensive cpu & network cost
            cost_Child


          case _ => throw new InvalidObjectException(u.toString)
        }
      }

      case b: BinaryNode =>
        val costLeftChild = estimateCost(b.left)
        val costRightChild = estimateCost(b.right)
        b match {
        case j @ Join(left, right, joinType, condition) =>{
          // broadcast join vs shuffle join should be handled differently

          costLeftChild
        }




        case u @ Union(left, right) => throw new NotImplementedException
        case e @ Except(left, right) => throw new NotImplementedException
        case i @ Intersect(left, right) => throw new NotImplementedException
        case _ =>throw new InvalidObjectException(b.toString)
      }
      case _ => throw new InvalidObjectException(plan.toString)
    }
  }

}
