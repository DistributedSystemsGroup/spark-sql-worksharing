package nw.fr.eurecom.dsg.optimizer

import nw.fr.eurecom.dsg.util.SparkSQLServerLogging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class Strategy(inPlans:Array[LogicalPlan], cachePlans:Array[LogicalPlan]) extends SparkSQLServerLogging{
  def execute(sQLContext: SQLContext):Array[DataFrame]={
    cachePlans.foreach(p =>{
      new DataFrame(sQLContext, p).cache()
      logInfo("Registered a cache plan: %s".format(p))
    })
    inPlans.map(p => new DataFrame(sQLContext, p))
  }

  override def toString():String ={
    var res = "Number of cache plan: %d".format(cachePlans.length)
    cachePlans.foreach(p => res = res + "\n" + p.toString())
    res
  }
}
