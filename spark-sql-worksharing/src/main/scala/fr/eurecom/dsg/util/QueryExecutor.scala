package fr.eurecom.dsg.util

import org.apache.spark.sql.extensions.optimizer.CacheAwareOptimizer
import org.apache.spark.sql.{SQLContext, DataFrame}

object QueryExecutor {

  /**
    * Execute queries sequentially (without Work Sharing) then execute queries with Work Sharing
    *
    * @param sqlContext
    * @param dfs queries in the form of dataframes
    * @param outputPath
    */
  def execute(sqlContext: SQLContext, dfs: Seq[DataFrame], outputPath: String): Unit = {
    executeSequential(sqlContext, dfs, outputPath)
    executeWorkSharing(sqlContext, dfs, outputPath)
  }

  /**
    * Execute queries sequentially (without Work Sharing)
    *
    * @param sqlContext
    * @param dfs        queries in the form of dataframes
    * @param outputPath output folder
    */
  def executeSequential(sqlContext: SQLContext, dfs: Seq[DataFrame], outputPath: String) = {
    dfs.zipWithIndex.foreach { case (df, i) => df.write.format("csv").option("header", "true").save(outputPath + "/_sequential_" + i.toString) }
  }

  /**
    * Execute queries sequentially with Work Sharing
    *
    * @param sqlContext
    * @param dfs queries in the form of dataframes
    * @param outputPath
    */
  def executeWorkSharing(sqlContext: SQLContext, dfs: Seq[DataFrame], outputPath: String) = {
    // take the optimized logical plans
    val optimizedLogicalPlans = dfs.map(df => df.queryExecution.optimizedPlan).toArray

    // optimize them
    val strategyGenerator = CacheAwareOptimizer.optimizeWithWorkSharing(optimizedLogicalPlans)
    val bestStrategy = strategyGenerator.get()

    println("Total profit = " + strategyGenerator.getProfit())
    println("Total weight = " + strategyGenerator.getWeight())

    val optimizedDFs = bestStrategy.execute(sqlContext)

    // execute the queries
    optimizedDFs.indices.foreach(i => optimizedDFs(i).write.format("csv")
      .option("header", "true")
      .save(outputPath + "/_worksharing_" + i.toString))
  }
}
