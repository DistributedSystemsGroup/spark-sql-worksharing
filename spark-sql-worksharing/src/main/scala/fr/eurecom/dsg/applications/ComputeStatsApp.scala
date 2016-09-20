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
    if(args.length != 4){
      System.out.println("Usage: <master> <inputDir> <savePath> <format>")
      System.exit(0)
    }

    val master = args(0)
    val inputDir = args(1)
    val savePath = args(2)
    val format = args(3)

    val conf = new SparkConf().setAppName(this.getClass.toString)
    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlc= new SQLContext(sc)

    // Use all tables
    val tables = Tables.getAllTables()
    // Use just some tables
    //val tables = Tables.getSomeTables()

    // QueryProvider will register your tables to the catalog system, so that your queries can be parsed and understood
    val queryProvider = new QueryProvider(sqlc, inputDir, tables, format)
    val stats = new StatisticsProvider()
    // We have 2 options:
    // - collect (compute) stats. You can save the result to a json file
    // - read from a json file where stats are pre-computed and written back to

    stats.collect(tables, queryProvider = queryProvider)

    stats.saveToFile(savePath)

    val newStats = stats.readFromFile(savePath)
    sc.stop()

//    while(true){
//
//    }
  }
}
