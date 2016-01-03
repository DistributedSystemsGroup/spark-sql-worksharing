package nw.fr.eurecom.dsg.optimizer

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by ntkhoa on 12/12/15.
  */
class Strategy(inPlans:Array[LogicalPlan], cachePlans:Array[LogicalPlan]){
  def execute(sQLContext: SQLContext):Array[DataFrame]={
    cachePlans.foreach(p =>{
      new DataFrame(sQLContext, p).cache()
      println("Cached the plan " + p.toString())
    })

    inPlans.foreach(println)

    inPlans.map(p => new DataFrame(sQLContext, p))
  }

  override def toString():String ={
    var res = "Number of cache plan: %d".format(cachePlans.length)
    cachePlans.foreach(p => res = res + "\n" + p.toString())
    res
  }


}
