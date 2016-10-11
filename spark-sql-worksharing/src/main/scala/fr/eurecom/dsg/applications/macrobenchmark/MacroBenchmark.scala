package fr.eurecom.dsg.applications.macrobenchmark

import fr.eurecom.dsg.statistics.StatisticsProvider
import fr.eurecom.dsg.util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.extensions.cost.CostEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Application for doing macro-benchmark
  * Work Sharing Prototype on Spark SQL
  */
object MacroBenchmark {
  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      System.out.println("Usage: <master> <inputDir> <outputDir> <format> <statFile> <mode> <nQueries> <randomlySelect> <seeds>")
      System.exit(0)
    }

    val master = args(0)
    val inputDir = args(1)
    val outputDir = args(2)
    val format = args(3)
    val statFile = args(4)
    val mode = args(5)
    val nQueries = args(6).toInt
    val randomlySelect = args(7).toBoolean
    val seeds = args(8).toInt

    val appName = "%s %s %s %s %d %s %d".format(this.getClass.getSimpleName, inputDir, format, mode, nQueries, randomlySelect.toString, seeds)
    val conf = new SparkConf().setAppName(appName)

    if (master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    sqlc.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true")
    sqlc.setConf("spark.sql.parquet.filterPushdown", "false")

    val tables = Tables.getAllTables()
    val queryProvider = new QueryProvider(sqlc, inputDir, tables, format)

    val stats = new StatisticsProvider().readFromFile(statFile)
    CostEstimator.setStatsProvider(stats)
    println("Statistics data is loaded!")

    val isNotParsedAble = Array("q16", "q23a", "q32", "q41", "q92", "q95")
    val notRunnable = Array("q9")
    val useLocalRelations = Array("q4", "q11", "q74")
    val ifelse = Array("q34", "q73")
    val attributeIDProblem = Array("q2", "q17", "q18", "q25", "q29", "q31", "q37", "q38", "q39a", "q39b", "q44", "q47", "q50", "q51", "q57", "q59", "q65", "q72", "q75", "q85", "q87")
    // same table and columns but produce multiple id
    val notSupportedFunctions = Array("q5", "q8", "q14a", "q10", "q24b", "q49", "q64", "q56", "q60", "q76", "q77", "q91", "q24a", "q23b", "q78", "q35", "q80", "q6", "q14b", "q54", "q58")
    // q5: cast(0 as decimal(7,2)) as return_amt => cannot be cached (bug)
    // q8: substring(ca_zip#477, 1, 5) EqualTo 56910
    // q14a, q10: left semi join
    // q24b, q49, q64: join with 2 conditions
    // q6, q14b, q54, q58: has subquery
    // q77: (isnotnull(d_date#518) && null)
    // q91: startWith

    val runableQueries = tpcds.tpcds1_4Queries.filter(q => !isNotParsedAble.contains(q._1)
      && !notRunnable.contains(q._1)
      && !useLocalRelations.contains(q._1)
      && !ifelse.contains(q._1)
      && !attributeIDProblem.contains(q._1)
      && !notSupportedFunctions.contains(q._1)
    )

    println("#runable queries: " + runableQueries.length)

    var queriesToRun: Seq[(String, String)] = null
    if (randomlySelect) {
      val r = new scala.util.Random(seeds)
      queriesToRun = r.shuffle(runableQueries).take(nQueries)
    }
    else {
      queriesToRun = runableQueries.take(nQueries)
    }

    println("queries to run: " + queriesToRun.map(_._1).mkString(" "))

    mode match {
      case "opt" => {
        QueryExecutor.executeWorkSharing(sqlc, queriesToRun.map(x => queryProvider.getDF(x._2)), outputDir)
        Emailer.sendMessage("Job done", "Pls check the cache amount on webui")
        Thread.sleep(60000) // sleep for 1 min to check cache usage on web ui
      }
      case "wopt" => QueryExecutor.executeSequential(sqlc, queriesToRun.map(x => queryProvider.getDF(x._2)), outputDir)
    }

    sc.stop()
  }
}
