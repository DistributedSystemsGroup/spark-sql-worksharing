import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object GenApp {
  def main(args: Array[String]) {
    if(args.length < 4){
      System.err.println("Usage: <inputPath> <scaleFactor> <format> <dsdgenDir>")
      // scaleFactor: number of GB of data to be generated
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName(this.getClass.getName())
    //conf.setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    // You need the HiveContext to be able to fully parse the queries
    val sqlContext= new org.apache.spark.sql.hive.HiveContext(sparkContext)

    // Tables in TPC-DS benchmark used by experiments.
    val tables = new Tables(
      sqlContext =  sqlContext,
      dsdgenDir = args(3),
      scaleFactor = args(1).toInt
    )

    tables.genData(
      location = args(0),
      format = args(2),
      overwrite = false,
      partitionTables = true,
      useDoubleForDecimal = false,
      clusterByPartitionColumns = true,
      filterOutNullPartitionValues = false
    )
  }
}
