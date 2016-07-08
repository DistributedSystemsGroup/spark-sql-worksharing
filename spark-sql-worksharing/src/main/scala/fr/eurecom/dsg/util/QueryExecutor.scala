package fr.eurecom.dsg.util

import org.apache.spark.sql.extensions.optimizer.CacheAwareOptimizer
import org.apache.spark.sql.{SQLContext, DataFrame}

object QueryExecutor{

  def execute(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String): Unit ={
    executeSequential(sqlContext, dfs, outputPath + "_seq")
    executeWorkSharing(sqlContext, dfs, outputPath + "_ws")
  }

  def executeSequential(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    dfs.zipWithIndex.foreach{case(df,i) => df.write.format("csv").option("header", "true").save(outputPath + "_sequential_" + i.toString)}
  }

  def executeWorkSharing(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    val plans = dfs.map(df => df.queryExecution.optimizedPlan).toArray
    val strategyGenerator = CacheAwareOptimizer.optimizePlans(plans)
    val bestStrategy = strategyGenerator.get()
    println("profit = " + strategyGenerator.getProfit())
    println("weight = " + strategyGenerator.getWeight())

    val df = bestStrategy.execute(sqlContext)

    df.indices.foreach(i => df(i).write.format("csv")
      .option("header", "true")
      .save(outputPath + "_worksharing_" + i.toString))
  }
}
