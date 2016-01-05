package nw.fr.eurecom.dsg

import nw.fr.eurecom.dsg.util.QueryProvider
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ArrayBuffer

/**
  * Internal usage only, for analyzing the LogicalPlans of queries
  */
object PlanViewer {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.out.println("Usage: <inputDir> <outputDir> <format>")
      System.exit(0)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val format = args(2)

    val conf = new SparkConf().setAppName(this.getClass.toString)
    //conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlc = new org.apache.spark.sql.hive.HiveContext(sc)

    sqlc.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true")
    sqlc.setConf("spark.sql.parquet.filterPushdown", "false")

    import sqlc.implicits._

    val tables = Seq("date_dim", "store_sales", "item", "store")
    val query = new QueryProvider(sqlc, inputDir, tables, format)

    val q = new ArrayBuffer[String]()

    val q3 =
      """
        | SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
        | FROM  date_dim dt, store_sales, item
        | WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
        |   AND store_sales.ss_item_sk = item.i_item_sk
        |   AND item.i_manufact_id = 128
        |   AND dt.d_moy=11
        | GROUP BY dt.d_year, item.i_brand, item.i_brand_id
        | ORDER BY dt.d_year, sum_agg desc, brand_id
        | LIMIT 100
      """.stripMargin



    val q42 =
      """
        | select dt.d_year, item.i_category_id, item.i_category, sum(ss_ext_sales_price)
        | from      date_dim dt, store_sales, item
        | where dt.d_date_sk = store_sales.ss_sold_date_sk
        |     and store_sales.ss_item_sk = item.i_item_sk
        |     and item.i_manager_id = 1
        |     and dt.d_moy=11
        |     and dt.d_year=2000
        | group by  dt.d_year
        |           ,item.i_category_id
        |           ,item.i_category
        | order by       sum(ss_ext_sales_price) desc,dt.d_year
        |           ,item.i_category_id
        |           ,item.i_category
        | limit 100
      """.stripMargin

    val q43 =
      """
        | select s_store_name, s_store_id,
        |        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        |        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        |        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        |        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        |        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        |        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        |        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
        | from date_dim, store_sales, store
        | where d_date_sk = ss_sold_date_sk and
        |       s_store_sk = ss_store_sk and
        |       s_gmt_offset = -5 and
        |       d_year = 2000
        | group by s_store_name, s_store_id
        | order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,
        |          thu_sales,fri_sales,sat_sales
        | limit 100
      """.stripMargin
    val q52 =
      """
        | select dt.d_year
        |     ,item.i_brand_id brand_id
        |     ,item.i_brand brand
        |     ,sum(ss_ext_sales_price) ext_price
        | from date_dim dt, store_sales, item
        | where dt.d_date_sk = store_sales.ss_sold_date_sk
        |    and store_sales.ss_item_sk = item.i_item_sk
        |    and item.i_manager_id = 1
        |    and dt.d_moy=11
        |    and dt.d_year=2000
        | group by dt.d_year, item.i_brand, item.i_brand_id
        | order by dt.d_year, ext_price desc, brand_id
        |limit 100
      """.stripMargin

    val q55 =
      """
        |select i_brand_id brand_id, i_brand brand,
        |     sum(ss_ext_sales_price) ext_price
        | from date_dim, store_sales, item
        | where d_date_sk = ss_sold_date_sk
        |     and ss_item_sk = i_item_sk
        |     and i_manager_id=28
        |     and d_moy=11
        |     and d_year=1999
        | group by i_brand, i_brand_id
        | order by ext_price desc, brand_id
        | limit 100
      """.stripMargin

    q.append(q3, q42, q43, q52, q55)
    q.foreach { str =>
      val df = query.getDF(str)
      println(df.queryExecution.optimizedPlan.toString())
    }
  }

}
