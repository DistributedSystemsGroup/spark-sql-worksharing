package fr.eurecom.dsg.applications

import fr.eurecom.dsg.statistics.StatisticsProvider
import fr.eurecom.dsg.util.{QueryProvider, Tables}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Launch this job to compute the statistics and save the result to file
  */
object ComputeStatsApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.out.println("Usage: <master> <inputDir> <savePath> <format>")
      System.exit(0)
    }

    val master = args(0) // {local, cluster}
    val inputDir = args(1) // where to read the input data
    val savePath = args(2) // where to save the statistics (output)
    val format = args(3) // {csv, parquet}

    val appName = "%s %s %s %s".format(this.getClass.getSimpleName, inputDir, savePath, format)

    val conf = new SparkConf().setAppName(appName)
    if (master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    val tableNames = Tables.getAllTables()

    // QueryProvider will register your tables to the catalog system, so that queries can be parsed and understood
    val queryProvider = new QueryProvider(sqlc, inputDir, tableNames, format)

    val statsProvider = new StatisticsProvider()
    statsProvider.collect(tableNames, queryProvider = queryProvider)
    statsProvider.saveToFile(savePath)

    // Try to retrieve statistics back
    val computedStats = statsProvider.readFromFile(savePath)

    sc.stop()
  }
}
