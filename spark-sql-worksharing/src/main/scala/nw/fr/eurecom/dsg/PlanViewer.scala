package org.apache.spark.sql


import com.databricks.spark.csv.CsvRelation
import nw.fr.eurecom.dsg.util.QueryProvider
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, UnaryNode, BinaryNode, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
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
    conf.setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    sqlc.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true")
    sqlc.setConf("spark.sql.parquet.filterPushdown", "false")

    import sqlc.implicits._

    val tables = Seq("catalog_sales", "catalog_returns",
      "inventory", "store_sales", "store_returns", "web_sales", "web_returns",
      "call_center", "catalog_page", "customer", "customer_address", "customer_demographics",
      "date_dim", "household_demographics", "income_band", "item", "promotion", "reason",
      "ship_mode", "store", "time_dim", "warehouse", "web_page", "web_site")
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

    val q7 =
      """
        | SELECT i_item_id,
        |        avg(ss_quantity) agg1,
        |        avg(ss_list_price) agg2,
        |        avg(ss_coupon_amt) agg3,
        |        avg(ss_sales_price) agg4
        | FROM store_sales, customer_demographics, date_dim, item, promotion
        | WHERE ss_sold_date_sk = d_date_sk AND
        |       ss_item_sk = i_item_sk AND
        |       ss_cdemo_sk = cd_demo_sk AND
        |       ss_promo_sk = p_promo_sk AND
        |       cd_gender = 'M' AND
        |       cd_marital_status = 'S' AND
        |       cd_education_status = 'College' AND
        |       (p_channel_email = 'N' or p_channel_event = 'N') AND
        |       d_year = 2000
        | GROUP BY i_item_id
        | ORDER BY i_item_id LIMIT 100
      """.stripMargin

    val q15 =
      """
        | select ca_zip, sum(cs_sales_price)
        | from catalog_sales, customer, customer_address, date_dim
        | where cs_bill_customer_sk = c_customer_sk
        |     and c_current_addr_sk = ca_address_sk
        |     and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
        |                                   '85392', '85460', '80348', '81792')
        |           or ca_state in ('CA','WA','GA')
        |           or cs_sales_price > 500)
        |     and cs_sold_date_sk = d_date_sk
        |     and d_qoy = 2 and d_year = 2001
        | group by ca_zip
        | order by ca_zip
        | limit 100
      """.stripMargin

    val q19 =
      """
        | select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
        |     sum(ss_ext_sales_price) ext_price
        | from date_dim, store_sales, item,customer,customer_address,store
        | where d_date_sk = ss_sold_date_sk
        |   and ss_item_sk = i_item_sk
        |   and i_manager_id = 8
        |   and d_moy = 11
        |   and d_year = 1998
        |   and ss_customer_sk = c_customer_sk
        |   and c_current_addr_sk = ca_address_sk
        |   and substr(ca_zip,1,5) <> substr(s_zip,1,5)
        |   and ss_store_sk = s_store_sk
        | group by i_brand, i_brand_id, i_manufact_id, i_manufact
        | order by ext_price desc, brand, brand_id, i_manufact_id, i_manufact
        | limit 100
      """.stripMargin

    val q21 =
      """
        | select * from(
        |   select w_warehouse_name, i_item_id,
        |          sum(case when (cast(d_date as date) < cast ('2000-03-11' as date))
        |                       then inv_quantity_on_hand
        |                   else 0 end) as inv_before,
        |          sum(case when (cast(d_date as date) >= cast ('2000-03-11' as date))
        |                   then inv_quantity_on_hand
        |                   else 0 end) as inv_after
        |   from inventory, warehouse, item, date_dim
        |   where i_current_price between 0.99 and 1.49
        |     and i_item_sk          = inv_item_sk
        |     and inv_warehouse_sk   = w_warehouse_sk
        |     and inv_date_sk        = d_date_sk
        |     and d_date between date_sub(cast('2000-03-11' as date), 30)
        |                    and date_add(cast('2000-03-11' as date), 30)
        |   group by w_warehouse_name, i_item_id) x
        | where (case when inv_before > 0
        |             then inv_after / inv_before
        |             else null
        |             end) between 2.0/3.0 and 3.0/2.0
        | order by w_warehouse_name, i_item_id
        | limit 100
      """.stripMargin

    val q26 =
      """
        | select i_item_id,
        |        avg(cs_quantity) agg1,
        |        avg(cs_list_price) agg2,
        |        avg(cs_coupon_amt) agg3,
        |        avg(cs_sales_price) agg4
        | from catalog_sales, customer_demographics, date_dim, item, promotion
        | where cs_sold_date_sk = d_date_sk and
        |       cs_item_sk = i_item_sk and
        |       cs_bill_cdemo_sk = cd_demo_sk and
        |       cs_promo_sk = p_promo_sk and
        |       cd_gender = 'M' and
        |       cd_marital_status = 'S' and
        |       cd_education_status = 'College' and
        |       (p_channel_email = 'N' or p_channel_event = 'N') and
        |       d_year = 2000
        | group by i_item_id
        | order by i_item_id
        | limit 100
      """.stripMargin

    val q37 =
      """
        | select i_item_id, i_item_desc, i_current_price
        | from item, inventory, date_dim, catalog_sales
        | where i_current_price between 68 and 68 + 30
        |   and inv_item_sk = i_item_sk
        |   and d_date_sk=inv_date_sk
        |   and d_date between cast('2000-02-01' as date) and date_add(cast('2000-02-01' as date), 60)
        |   and i_manufact_id in (677,940,694,808)
        |   and inv_quantity_on_hand between 100 and 500
        |   and cs_item_sk = i_item_sk
        | group by i_item_id,i_item_desc,i_current_price
        | order by i_item_id
        | limit 100
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

    val q82 =
      """
        | select i_item_id, i_item_desc, i_current_price
        | from item, inventory, date_dim, store_sales
        | where i_current_price between 62 and 62+30
        |   and inv_item_sk = i_item_sk
        |   and d_date_sk=inv_date_sk
        |   and d_date between cast('2000-05-25' as date) and date_add(cast('2000-05-25' as date), 60)
        |   and i_manufact_id in (129, 270, 821, 423)
        |   and inv_quantity_on_hand between 100 and 500
        |   and ss_item_sk = i_item_sk
        | group by i_item_id,i_item_desc,i_current_price
        | order by i_item_id
        | limit 100
      """.stripMargin

    val q96 =
      """
        | select count(*)
        | from store_sales, household_demographics, time_dim, store
        | where ss_sold_time_sk = time_dim.t_time_sk
        |     and ss_hdemo_sk = household_demographics.hd_demo_sk
        |     and ss_store_sk = s_store_sk
        |     and time_dim.t_hour = 20
        |     and time_dim.t_minute >= 30
        |     and household_demographics.hd_dep_count = 7
        |     and store.s_store_name = 'ese'
        | order by count(*)
        | limit 100
      """.stripMargin


    q.append(q3, q7, q15, q19, q21, q26, q37, q42, q43, q52, q55, q82, q96)
    q.foreach { str =>
      val df = query.getDF(str)
      println(getVisualizedString(df.queryExecution.optimizedPlan))
      println(df.queryExecution.optimizedPlan)
      //println(df.queryExecution.optimizedPlan.toString())
    }
  }

  /**
    * Use the format of the following site
    * http://ironcreek.net/phpsyntaxtree/?
    * Paste the result and you can get the image of the tree visualized
    * @param p
    * @return
    */
  def getVisualizedString(p:LogicalPlan):String ={
    val opName = p.getClass.getSimpleName

    p match {
      case b: BinaryNode =>
        return "[%s %s %s]".format(opName, getVisualizedString(b.left), getVisualizedString(b.right))
      case u: UnaryNode =>
        return "[%s %s]".format(opName, getVisualizedString(u.child))
      case l: LeafNode =>
        val path = l.asInstanceOf[LogicalRelation].relation.asInstanceOf[CsvRelation].location.get
        return "%s".format(path.substring(path.lastIndexOf('/')+1))
    }
    ""
  }


}
