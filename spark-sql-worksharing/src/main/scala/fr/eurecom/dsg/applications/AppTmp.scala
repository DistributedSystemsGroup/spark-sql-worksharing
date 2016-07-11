package fr.eurecom.dsg.applications

import fr.eurecom.dsg.statistics.StatisticsProvider
import fr.eurecom.dsg.util.{tpcds, Tables, QueryExecutor, QueryProvider}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.extensions.cost.CostEstimator
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Demo application of the CacheAware (MQO) Optimizer
  * Entry point
  */
object AppTmp {
  def main(args: Array[String]): Unit = {
    if(args.length != 5){
      System.out.println("Usage: <master> <inputDir> <outputDir> <format> <statFile>")
      System.exit(0)
    }

    val master = args(0)
    val inputDir = args(1)
    val outputDir = args(2)
    val format = args(3)
    val statFile = args(4)

    val conf = new SparkConf().setAppName(this.getClass.toString)
    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
//    val sqlc= new org.apache.spark.sql.hive.HiveContext(sc)
    val sqlc = new SQLContext(sc)

    sqlc.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true")
    sqlc.setConf("spark.sql.parquet.filterPushdown", "false")

    val tables = Tables.getSomeTables()
    val queryProvider = new QueryProvider(sqlc, inputDir, tables, format)

    val stats = new StatisticsProvider().readFromFile(statFile)
    CostEstimator.setStatsProvider(stats)
    println("Statistics data is loaded!")

    val df3 = queryProvider.getDF(tpcds.tpcds1_4Queries.filter(q => q._1 == "q3").take(1).head._2)

    df3.queryExecution.optimizedPlan

    val df42 = queryProvider.getDF(tpcds.tpcds1_4Queries.filter(q => q._1 == "q42").take(1).head._2)

//    QueryExecutor.executeSequential(sqlc, Seq(df3, df42), outputDir)

//    QueryExecutor.executeWorkSharing(sqlc, Seq(df3, df42), outputDir)


    sc.stop()

  }
}
