package org.apache.spark.sql.extensions

import java.io.InvalidObjectException

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.sources.BaseRelation

/**
  * Created by ntkhoa on 26/05/16.
  */
object Util {
  def toDataFrame(sparkSession: SparkSession, logicalPlan:LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def stringToInt(key:String, mod:Int):Int = {
    val res = key.hashCode() % mod
    if(res < 0)
      res + mod
    else
      res
  }

  def extractTableName(r: BaseRelation): String = {
    r match {
      case r:HadoopFsRelation => r.location.paths.iterator.next.getName
      case _ => throw new InvalidObjectException("cannot extract table name from relation" + r.toString)
    }
  }
}
