import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by ntkhoa on 24/05/16.
  */
object App {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getName())
    conf.setMaster("local[2]")
    val sparkContext = new SparkContext(conf)

    // You need the HiveContext to be able to fully parse the queries
    val sqlContext= new SQLContext(sparkContext)
    val df = sqlContext.read.option("header", "true").csv("/home/ntkhoa/people.csv")
    val df2 = sqlContext.read.parquet("/home/ntkhoa/users.parquet")
    val df3 = sqlContext.read.json("/home/ntkhoa/people.json")

    df.collect()
  }

}
