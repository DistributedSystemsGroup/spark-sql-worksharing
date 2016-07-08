import com.databricks.spark.sql.perf.tpcds.{TPCDS}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import util.QueryProvider
import util.Tables

object BenchmarkApp {
  def main(args: Array[String]) {
    if (args.length != 5) {
      System.err.println("Usage: <master> <inputPath> <outputPath> <format> <iterations>")
      // scaleFactor: number of GB of data to be generated
      System.exit(-1)
    }

    val master = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val format = args(3)
    val iteration = args(4).toInt

    val conf = new SparkConf().setAppName(this.getClass.getName())
    conf.set("spark.sql.perf.results", outputPath)

    if(master.toLowerCase == "local")
      conf.setMaster("local[2]")

    val sparkContext = new SparkContext(conf)

    // You need the HiveContext to be able to fully parse the queries
    val sqlContext = new SQLContext(sparkContext)
    val tables = Tables.getAllTables()
    val queryProvider = new QueryProvider(sqlContext, inputPath, tables, format)

    val tpcds = new TPCDS(sqlContext = sqlContext)

    val queries = tpcds.tpcds1_4Queries

    tpcds.runExperiment(
      executionsToRun = queries,
      iterations = iteration
    )

    // block the ui
    while (true) {

    }
  }
}
