import com.databricks.spark.sql.perf.tpcds.{TPCDS, Tables}
import org.apache.spark.sql.catalyst.plans.logical.Limit
import org.apache.spark.{SparkContext, SparkConf}

object BenchmarkApp {
  def main(args: Array[String]) {

    if(args.length < 6){
      System.err.println("Usage: <inputPath> <outputPath> <scaleFactor> <format> <iterations> <dsdgenDir>")
      // scaleFactor: number of GB of data to be generated
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName(this.getClass.getName())
    conf.setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    // You need the HiveContext to be able to fully parse the queries
    val sqlContext= new org.apache.spark.sql.hive.HiveContext(sparkContext)

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

    //, , , item, promotion
//    val df = sqlContext.sql("""
//                     | SELECT *
//                     | FROM store_sales
//                   """.stripMargin)
//
//    val df2 = sqlContext.sql("""
//                              | SELECT *
//                              | FROM customer_demographics
//                            """.stripMargin)
//
//    val df3 = sqlContext.sql("""
//                              | SELECT *
//                              | FROM date_dim
//                            """.stripMargin)


    val tpcds = new TPCDS(
      sqlContext = sqlContext,
      resultsLocation = args(1)
    )

    var queries = tpcds.q7Derived

    val exp = tpcds.runExperiment(
      executionsToRun = queries,
      includeBreakdown = true,
      iterations = args(4).toInt
    )

    exp.waitForFinish(Int.MaxValue)

    //      tpcds.createResultsTable()

    sparkContext.stop()

  }
}
