package fr.eurecom.dsg.applications.microbenchmark

import fr.eurecom.dsg.applications.microbenchmark.queries.{SimpleProjection, SimpleFiltering, MicroBQuery}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, StringType, DoubleType}
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ntkhoa on 30/06/16.
  */
object MicroBenchmark {
  def main(args: Array[String]) {
    val master = args(0)
    val inputFile = args(1)
    val format = args(2)

    val conf = new SparkConf().setAppName(this.getClass.toString)
    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true") // them cai nay vao bao cao
    sqlContext.setConf("spark.sql.parquet.filterPushdown", "false")

    import sqlContext.implicits._
    var data:DataFrame = null

    if(format == "csv"){
      data = sqlContext.read.schema(DataRow.getSchema).csv(inputFile)
      data.printSchema()
    }
    else{
      data = sqlContext.read.parquet(inputFile)
      data.printSchema()
    }

    val q = new SimpleProjection(data)

//    q.runWithoutOpt()
    q.runWithOpt()


    sc.stop()

  }
}


