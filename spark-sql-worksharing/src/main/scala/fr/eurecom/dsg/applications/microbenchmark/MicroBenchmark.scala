package fr.eurecom.dsg.applications.microbenchmark

import com.sun.javaws.exceptions.InvalidArgumentException
import fr.eurecom.dsg.applications.microbenchmark.queries._
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

    var q:MicroBQuery = null
    query match{
      case 0 => q = new SimpleFiltering(data)
      case 1 => q = new SimpleProjection(data)
      case 2 => q = new SimpleFilteringProjection(data)
      case 3 => q = new SimpleJoining(data)
      case _ => throw new IllegalArgumentException("query = " + query.toString)
    }

    mode match{
      case "opt" => q.runWithOpt()
      case "wopt" => q.runWithoutOpt()
      case _ => throw new IllegalArgumentException("mode = " + mode)
    }

    sc.stop()

  }
}


