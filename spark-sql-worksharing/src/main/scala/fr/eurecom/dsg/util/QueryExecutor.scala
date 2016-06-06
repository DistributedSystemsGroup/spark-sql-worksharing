package fr.eurecom.dsg.util

import org.apache.spark.sql.extensions.optimizer.CacheAwareOptimizer
import org.apache.spark.sql.{SQLContext, DataFrame}

object QueryExecutor{

  def execute(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String): Unit ={
    executeSequential(sqlContext, dfs, outputPath + "_seq")
    executeWorkSharing(sqlContext, dfs, outputPath + "_ws")
  }

  def executeSequential(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    dfs.zipWithIndex.foreach{case(df,i) => df.write.format("csv").save(outputPath + "_seqential_" + i.toString)}
  }

  def executeWorkSharing(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    val plans = dfs.map(df => df.queryExecution.optimizedPlan).toArray
    val strategyGenerator = CacheAwareOptimizer.optimizePlans(plans, sqlContext)
    val bestStrategy = strategyGenerator.get(4)
    println("profit = " + strategyGenerator.CEs(4).profit)
    println("weight = " + strategyGenerator.CEs(4).weight)


    val df = bestStrategy.execute(sqlContext)
    df.indices.foreach(i => df(i).write.format("csv")
      .option("header", "true")
      .save(outputPath + "_worksharing_" + i.toString))
  }

//  def executeWorkSharing(strategyIndex:Int, sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String): Unit ={
//    val plans = dfs.map(df => df.queryExecution.optimizedPlan).toArray
//    val strategyGenerator = CacheAwareOptimizer.optimizePlans(plans, sqlContext)
//    var i = -1
//
//    while(strategyGenerator.hasNext()){
//      val bestStrategy = strategyGenerator.next()
//      i+=1
//      if(i == strategyIndex){
//        val df = bestStrategy.execute(sqlContext)
//            df.indices.foreach(i => df(i).write.format("com.databricks.spark.csv")
//              .option("header", "true")
//              .save(outputPath + "/" + strategyIndex + "/"+ i.toString))
//      }
//    }
//
//  }

}
