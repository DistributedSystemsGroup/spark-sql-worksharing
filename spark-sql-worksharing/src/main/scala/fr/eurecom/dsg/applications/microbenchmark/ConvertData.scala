package fr.eurecom.dsg.applications.microbenchmark
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Application to convert txt format to csv and parquet format
  */
object ConvertData {
  def main(args: Array[String]) {
    val master = args(0)
    val inputFile = args(1)
    val outputPath = args(2)

    val appName = "%s %s %s".format(this.getClass.getSimpleName, inputFile, outputPath)
    val conf = new SparkConf().setAppName(appName)

    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val logData = sc.textFile(inputFile).map(line => new DataRow(line.split(" "))).toDS()

    logData.write.csv(outputPath + "-csv")
    logData.write.parquet(outputPath + "-parquet")

    sc.stop()
  }

}
