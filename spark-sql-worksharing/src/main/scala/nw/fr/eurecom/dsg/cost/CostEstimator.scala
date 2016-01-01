package org.apache.spark.sql

import com.databricks.spark.csv.CsvRelation
import nw.fr.eurecom.dsg.cost.Estimation
import nw.fr.eurecom.dsg.statistics.StatisticsProvider
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.json.JSONRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.sources.{TableScan, BaseRelation}


object CostEstimator {
  var statsProvider:StatisticsProvider = null
  def setStatsProvider(provider: StatisticsProvider): Unit ={
    statsProvider = provider
  }

  /**
    * estimate the cost of executing a given logical plan
    * Normally, this should be declared in the class of each operator
    * However, we want the framework to be an extension, without modifying the core of Spark/ Spark SQL
    * So, we implement here
    * @param plan logicalPlan (tree)
    * @return estimated cost of evaluating this plan
    */
  def estimateCost(plan:LogicalPlan):Estimation={
    plan match{
      case l:LeafNode => l match{
        case l @ LogicalRelation(baseRelation:BaseRelation, _) =>
          val est = new Estimation()
          baseRelation match{
            case r:JSONRelation =>
            case r: ParquetRelation =>
            case r:CsvRelation =>
          }


          return est
      }
      case u:UnaryNode => u match{
        case p:Project => val est = new Estimation()
          return est
        case f:Filter =>
        case d:Distinct =>
        case a:Aggregate =>
        case l:Limit =>
        case s:Sort =>

        case _ =>
      }

      case b:BinaryNode => b match{
        case j:Join =>
        case u:Union =>
        case e:Except =>
        case i:Intersect =>
        case _ =>
      }
      case _ =>
    }

    // traverse

    return null
  }

}
