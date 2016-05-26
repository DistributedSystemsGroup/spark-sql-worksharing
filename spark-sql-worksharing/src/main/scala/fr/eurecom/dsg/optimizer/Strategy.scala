package org.apache.spark.sql.myExtensions.optimizer

import fr.eurecom.dsg.util.SparkSQLServerLogging
import org.apache.spark.sql.{Dataset, DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class Strategy(inPlans:Array[LogicalPlan], cachePlans:Array[LogicalPlan]) extends SparkSQLServerLogging{
  def execute(sQLContext: SQLContext):Array[DataFrame]={
    cachePlans.foreach(p =>{
      Dataset.ofRows(sQLContext.sparkSession, p).cache()
      logInfo("Registered a cache plan: %s".format(p))
    })
    inPlans.map(p => Dataset.ofRows(sQLContext.sparkSession, p))
  }

  override def toString():String ={
    var res = "Number of cache plan: %d".format(cachePlans.length)
    cachePlans.foreach(p => res = res + "\n" + p.toString())
    res
  }
}
