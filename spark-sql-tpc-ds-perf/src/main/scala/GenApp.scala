import com.databricks.spark.sql.perf.tpcds.Tables
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Generate TPC-DS tables
  */
object GenApp {
  def main(args: Array[String]) {
    if(args.length != 5){
      System.err.println("Usage: <master> <outputPath> <scaleFactor> <format> <dsdgenDir>")
      // scaleFactor: number of GB of data to be generated
      System.exit(-1)
    }

    val master = args(0)
    val outputPath = args(1)
    val scaleFactor = args(2).toInt
    val format = args(3)
    val dsdgenDir = args(4)

    val conf = new SparkConf().setAppName(this.getClass.getName())
    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sparkContext = new SparkContext(conf)
    val sqlContext= new SQLContext(sparkContext)

    // Tables in TPC-DS benchmark used by experiments.
    val tables = new Tables(
      sqlContext =  sqlContext,
      dsdgenDir = dsdgenDir,
      scaleFactor = scaleFactor
    )

    tables.genData(
      location = outputPath,
      format = format,
      overwrite = false,
      partitionTables = false,
      useDoubleForDecimal = false,
      clusterByPartitionColumns = true,
      filterOutNullPartitionValues = false
    )

    sparkContext.stop()
  }
}
