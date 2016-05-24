import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

object BenchmarkApp {
  def main(args: Array[String]) {
    if(args.length < 6){
      System.err.println("Usage: <inputPath> <outputPath> <scaleFactor> <format> <iterations> <dsdgenDir>")
      // scaleFactor: number of GB of data to be generated
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName(this.getClass.getName())
    conf.set("spark.sql.perf.results", args(1))

    conf.setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    // You need the HiveContext to be able to fully parse the queries
    val sqlContext= new HiveContext(sparkContext)

    // Tables in TPC-DS benchmark used by experiments.
    val tables = new Tables(
      sqlContext =  sqlContext,
      dsdgenDir = args(5),
      scaleFactor = args(2).toInt
    )

    tables.createTemporaryTables(
      location = args(0),
      format = args(3)
    )

    val tpcds = new TPCDS(sqlContext = sqlContext)

    val queries = tpcds.tpcds1_4Queries

    tpcds.runExperiment(
      executionsToRun = queries,
      iterations = args(4).toInt
    )

    // block the ui
    while(true){

    }
  }
}
