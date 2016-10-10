package fr.eurecom.dsg.applications.microbenchmark

import fr.eurecom.dsg.applications.microbenchmark.queries._
import fr.eurecom.dsg.util.Emailer
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}


object MicroBenchmark {
  def main(args: Array[String]) {
    val master = args(0)
    val inputFile = args(1)
    val format = args(2)
    val query = args(3).toInt
    val mode = args(4)

    val conf = new SparkConf().setAppName("%s %s %s %s".format(this.getClass.getName, inputFile , query.toString, mode))
    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true") // them cai nay vao bao cao
    sqlContext.setConf("spark.sql.parquet.filterPushdown", "false")
    var data:DataFrame = null

    if(format == "csv"){
      data = sqlContext.read.schema(DataRow.getSchema).csv(inputFile)
      data.printSchema()
    }
    else{
      data = sqlContext.read.parquet(inputFile)
      data.printSchema()
    }

    var q:MicroBenchmarkExperiment = null
    query match{
      case 0 => q = new SimpleFiltering(data)
      case 1 => q = new SimpleProjection(data)
      case 2 => q = new SimpleFilteringProjection(data)
      case 3 => q ={
        val refTable = "data/random/ref-1M-"
        var refData:DataFrame = null
        if(format == "csv"){
          refData = sqlContext.read.schema(RefDataRow.getSchema).csv(refTable + "csv")
        }
        else{
          refData = sqlContext.read.schema(RefDataRow.getSchema).parquet(refTable + "parquet")
        }
        new SimpleJoining(data, refData)
      }
      case 4 => q ={
        val refTable = "data/random/ref-10M-"
        var refData:DataFrame = null
        if(format == "csv"){
          refData = sqlContext.read.schema(RefDataRow.getSchema).csv(refTable + "csv")
        }
        else{
          refData = sqlContext.read.schema(RefDataRow.getSchema).parquet(refTable + "parquet")
        }
        new SimpleJoining(data, refData)
      }

      case 100 => q = new SimpleFilteringFC(data)
      case 101 => q = new SimpleProjectionFC(data)
      case 102 => q = new SimpleFilteringProjectionFC(data)

      case _ => throw new IllegalArgumentException("query = " + query.toString)
    }

    mode match{
      case "opt" =>
        q.runWithWorkSharing()
        Emailer.sendMessage("Job done", "Pls check the cache amount on webui")
        Thread.sleep(60000) // sleep for 60 secs, so that I can check how much memory has been cached
      case "wopt" =>

        q.runWithoutWorkSharing()
      case _ => throw new IllegalArgumentException("mode = " + mode)
    }

//    while(true){
//      Thread.sleep(1000)
//    }

    sc.stop()

  }
}


