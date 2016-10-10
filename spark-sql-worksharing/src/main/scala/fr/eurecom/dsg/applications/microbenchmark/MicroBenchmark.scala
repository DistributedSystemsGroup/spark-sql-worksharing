package fr.eurecom.dsg.applications.microbenchmark

import fr.eurecom.dsg.applications.microbenchmark.queries._
import fr.eurecom.dsg.util.Emailer
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Application for doing micro-benchmark
  */
object MicroBenchmark {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.out.println("Usage: <master> <inputPath> <format> <experimentNum> <mode>")
      System.exit(0)
    }

    val master = args(0) // {local, cluster}
    val inputPath = args(1) // path to the input table
    val format = args(2) // {csv, parquet}
    val experimentNum = args(3).toInt // {0, 1, 2, 3, 4, 100, 101, 102}
    val mode = args(4) // {opt, wopt} - with work sharing / without work sharing

    val conf = new SparkConf().setAppName("%s %s %s %d %s".format(this.getClass.getName, inputPath, format, experimentNum, mode))
    if (master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false") // enabling this will affect the caching cost significantly
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true")
    sqlContext.setConf("spark.sql.parquet.filterPushdown", "false")

    var data: DataFrame = null

    format match {
      case "csv" => data = sqlContext.read.schema(DataRow.getSchema).csv(inputPath)
      case "parquet" => data = sqlContext.read.parquet(inputPath)
      case _ => throw new IllegalArgumentException(format)
    }

    //data.printSchema()

    var experiment: MicroBenchmarkExperiment = null
    experimentNum match {

      // Experiments on doing optimized cache on Similar Subexpressions
      case 0 => experiment = new SimpleFiltering(data)
      case 1 => experiment = new SimpleProjection(data)
      case 2 => experiment = new SimpleFilteringProjection(data)
      case 3 => experiment = {
        val refTable = "data/random/ref-1M-" // TODO: change this!!! too lazy to add parameter :(
        var refData: DataFrame = null
        format match {
          case "csv" => refData = sqlContext.read.schema(RefDataRow.getSchema).csv(refTable + "csv")
          case "parquet" => refData = sqlContext.read.schema(RefDataRow.getSchema).parquet(refTable + "parquet")
          case _ => throw new IllegalArgumentException(format)
        }

        new SimpleJoining(data, refData)
      }
      case 4 => experiment = {
        val refTable = "data/random/ref-10M-" // TODO: change this!!! too lazy to add parameter :(
        var refData: DataFrame = null

        format match {
          case "csv" => refData = sqlContext.read.schema(RefDataRow.getSchema).csv(refTable + "csv")
          case "parquet" => refData = sqlContext.read.schema(RefDataRow.getSchema).parquet(refTable + "parquet")
          case _ => throw new IllegalArgumentException(format)
        }

        new SimpleJoining(data, refData)
      }

      // Experiments on doing full cache on base tables
      case 100 => experiment = new SimpleFilteringFC(data)
      case 101 => experiment = new SimpleProjectionFC(data)
      case 102 => experiment = new SimpleFilteringProjectionFC(data)

      case _ => throw new IllegalArgumentException(experimentNum.toString)
    }

    mode match {
      case "opt" =>
        experiment.runWithWorkSharing()
        Emailer.sendMessage("Job done", "Pls check the cache amount on webui")
        Thread.sleep(60000) // sleep for 60 secs, so that I can check the cache usage on the Application UI
      case "wopt" => experiment.runWithoutWorkSharing()
      case _ => throw new IllegalArgumentException(mode)
    }

    sc.stop()
  }
}


