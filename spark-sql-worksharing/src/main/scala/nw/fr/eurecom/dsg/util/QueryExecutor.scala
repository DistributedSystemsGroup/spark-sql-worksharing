package nw.fr.eurecom.dsg.util

import org.apache.spark.sql.{SQLContext, CacheAwareOptimizer, DataFrame}

/**
  * Created by ntkhoa on 09/12/15.
  */
object QueryExecutor{

  def execute(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String): Unit ={
    executeSequential(sqlContext, dfs, outputPath)
    executeWorkSharing(sqlContext, dfs, outputPath)
  }

  def executeSequential(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    dfs.zipWithIndex.foreach{case(df,i) => df.write.save(outputPath + i.toString)}
  }

  def executeWorkSharing(sqlContext:SQLContext, dfs:Seq[DataFrame], outputPath:String)={
    val plans = dfs.map(df => df.queryExecution.optimizedPlan).toArray
    val strategyGenerator = CacheAwareOptimizer.optimizePlans(plans, sqlContext)
    val strategy = strategyGenerator.next()
    val df = strategy.execute(sqlContext)
    df.indices.foreach(i => df(i).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outputPath + "/" + i.toString))
  }

}
