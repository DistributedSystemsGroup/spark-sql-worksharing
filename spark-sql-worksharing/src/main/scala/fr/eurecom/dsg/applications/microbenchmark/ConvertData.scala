package fr.eurecom.dsg.applications.microbenchmark

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ntkhoa on 30/06/16.
  */
object ConvertData {
  def main(args: Array[String]) {
    val inputFile = args(0)

    val conf = new SparkConf().setAppName("Convert data").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val logData = sc.textFile(inputFile).map(line => new DataRow(line.split(" "))).toDS()

    logData.write.csv("/home/ntkhoa/micro/dat_micro_csv")
    logData.write.parquet("/home/ntkhoa/micro/dat_micro_parquet")
  }

}
