package fr.eurecom.dsg.applications

import fr.eurecom.dsg.statistics.StatisticsProvider
import fr.eurecom.dsg.util._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.extensions.cost.CostEstimator
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Demo application of the CacheAware (MQO) Optimizer
  * Entry point
  */
object AppTmp2 {
  def main(args: Array[String]): Unit = {
    if(args.length != 7){
      System.out.println("Usage: <master> <inputDir> <outputDir> <format> <statFile> <mode> <nQueries>")
      System.exit(0)
    }

    val master = args(0)
    val inputDir = args(1)
    val outputDir = args(2)
    val format = args(3)
    val statFile = args(4)
    val mode = args(5)
    val nQueries = args(6).toInt

    val conf = new SparkConf().setAppName("%s %s %s %s %s".format(this.getClass.getSimpleName, inputDir , format, mode, nQueries))
    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    //    val sqlc= new org.apache.spark.sql.hive.HiveContext(sc)
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

    // tpc-ds
    // 99 queries (104)
    // parsable: 104 - 6 = 98 (16, 23a, 32, 41, 92, 95)
    // benchmark failed: 97 (9 + not parsable)
    //

    val isNotParsedAble = Array("q16", "q23a", "q32", "q41", "q92", "q95")
    val notRunnable = Array("q9")
    val useLocalRelations = Array("q4", "q11", "q74")
    val ifelse = Array("q34", "q73") // case sth
    val attributeIDProblem = Array("q2", "q17", "q18", "q25", "q29", "q31", "q37", "q38", "q39a", "q39b", "q44", "q47", "q50", "q51", "q57", "q59", "q65", "q72", "q75", "q85", "q87") // same table and columns but produce multiple id
    // q2: d_year#1074, d_week_seq#1044, d_year#1046, d_week_seq#1072

    val strangeOrComplex = Array("q5") // q5 is over complex: cast(0 as decimal(7,2)) as return_amt => cannot be cached (bug)
    val hasSubQuery = Array("q6", "q14b", "q54", "q58")

    // (ok to use the default, remove the exception)
    val subString = Array("q8") //q8: substring(ca_zip#477, 1, 5) EqualTo 56910

    val leftsemijoin = Array("q14a")

    // can be fixed
    val join2Conditions = Array("q24b", "q49", "q64")
    val weird = Array("q56", "q60", "q76") // q56: too big subset => hang out
    val stupid = Array("q77") // q77: (isnotnull(d_date#518) && null)

    // can be fixed
    val startWith = Array("q91")

    val failedOurOptimization = Array("q24a", "q23b") // TODO: q10 changed physical execution strategy
    // 24a: failed to run in 50 SF

    println(tpcds.tpcds1_4Queries.length)

    val runableQueries = tpcds.tpcds1_4Queries.filter(q => !isNotParsedAble.contains(q._1)
      && !notRunnable.contains(q._1)
      && !useLocalRelations.contains(q._1)
      && !ifelse.contains(q._1)
      && !attributeIDProblem.contains(q._1)
      && !strangeOrComplex.contains(q._1)
      && !hasSubQuery.contains(q._1)
      && !subString.contains(q._1)
      && !leftsemijoin.contains(q._1)
      && !join2Conditions.contains(q._1)
      && !weird.contains(q._1)
      && !stupid.contains(q._1)
      && !startWith.contains(q._1)
      && !failedOurOptimization.contains(q._1)
    )
    println("#runable queries: " + runableQueries.length)

    println("queries to run: " + runableQueries.take(nQueries).map(_._1).mkString(" "))

    mode match{
      case "opt" => {
        QueryExecutor.executeWorkSharing(sqlc, runableQueries.take(nQueries).map(x => queryProvider.getDF(x._2)), outputDir)
        Emailer.sendMessage("Job done", "Pls check the cache amount on webui")
        Thread.sleep(120000) // sleep for 2 mins, so that I can check how much memory has been cached
      }
      case "wopt" => QueryExecutor.executeSequential(sqlc, runableQueries.take(nQueries).map(x => queryProvider.getDF(x._2)), outputDir)

    }


//    while(true){
//
//    }

    sc.stop()


    // observation: q1: a CTE



  }
}
