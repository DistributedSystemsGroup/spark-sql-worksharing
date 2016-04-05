package nw.fr.eurecom.dsg

import nw.fr.eurecom.dsg.statistics.StatisticsProvider
import nw.fr.eurecom.dsg.util.QueryProvider
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Launch this job to compute the statistics and save the result to file
  */
object ComputeStats {
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
//    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    //val sqlc = new SQLContext(sc)
    val sqlc= new org.apache.spark.sql.hive.HiveContext(sc)

    sqlc.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true")
    sqlc.setConf("spark.sql.parquet.filterPushdown", "false")
    import sqlc.implicits._

    // Uncomment the following block if you want to use all tables in your queries
    // These are all tables of the TPC-DS benchmark


        val tables = Seq("catalog_sales", "catalog_returns",
        "inventory", "store_sales", "store_returns", "web_sales", "web_returns",
        "call_center", "catalog_page", "customer", "customer_address", "customer_demographics",
        "date_dim", "household_demographics", "income_band", "item", "promotion", "reason",
        "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site")

    // Or uncomment the following block if you just want to use some tables in your queries
    //    val tables = Seq("customer", "customer_address", "customer_demographics",
    //      "date_dim", "item", "promotion", "store", "store_sales", "catalog_sales", "web_sales")

//    val tables = Seq("date_dim", "store_sales", "item")

    // QueryProvider will register your tables to the catalog system, so that your queries can be parsed
    // and understood
    val queryProvider = new QueryProvider(sqlc, inputDir, tables, format)
    val stats = new StatisticsProvider()
    // We have 2 options:
    // - collect (compute) stats. You can save the result to a json file
    // - read from a json file where stats are pre-computed and written back to

    stats.collect(tables, queryProvider = queryProvider)
    stats.saveToFile(savePath)
  }
}
