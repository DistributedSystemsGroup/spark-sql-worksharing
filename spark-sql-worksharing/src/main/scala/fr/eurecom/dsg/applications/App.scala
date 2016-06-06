package fr.eurecom.dsg.applications

import fr.eurecom.dsg.statistics.StatisticsProvider
import fr.eurecom.dsg.util.{Tables, QueryExecutor, QueryProvider, SparkSQLServerLogging}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.extensions.cost.CostEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Demo application of the CacheAware (MQO) Optimizer
  * Entry point
  */
object App extends SparkSQLServerLogging{
  def main(args: Array[String]): Unit = {
    if(args.length != 6){
      logError("Usage: <master> <inputDir> <outputDir> <format> <strategyIndex> <statFile>")
      System.exit(0)
    }

    val master = args(0)
    val inputDir = args(1)
    val outputDir = args(2)
    val format = args(3)
    val strategyIndex = args(4).toInt
    val statFile = args(5)


    val conf = new SparkConf().setAppName(this.getClass.toString)
    if(master.toLowerCase == "local")
      conf.setMaster("local[1]")

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)
    sqlc.setConf("spark.sql.parquet.cacheMetadata", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.compressed", "false")
    sqlc.setConf("spark.sql.inMemoryColumnarStorage.partitionPruning", "true")
    sqlc.setConf("spark.sql.parquet.filterPushdown", "false")

    val tables = Tables.getSomeTables()

    // QueryProvider will register your tables to the catalog system, so that your queries can be parsed
    // and understood
    val queryProvider = new QueryProvider(sqlc, inputDir, tables, format)

    val stats = new StatisticsProvider().readFromFile(statFile)
    CostEstimator.setStatsProvider(stats)
    logInfo("Loaded statistics data from file at %s".format(statFile))

//    val df2 = queryProvider.getDF("""
//                            | WITH wscs as
//                            | (SELECT sold_date_sk, sales_price
//                            |  FROM (SELECT ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price
//                            |        FROM web_sales) x
//                            |        UNION ALL
//                            |       (SELECT cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price
//                            |        FROM catalog_sales)),
//                            | wswscs AS
//                            | (SELECT d_week_seq,
//                            |        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
//                            |        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
//                            |        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
//                            |        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
//                            |        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
//                            |        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
//                            |        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
//                            | FROM wscs, date_dim
//                            | WHERE d_date_sk = sold_date_sk
//                            | GROUP BY d_week_seq)
//                            | SELECT d_week_seq1
//                            |       ,round(sun_sales1/sun_sales2,2)
//                            |       ,round(mon_sales1/mon_sales2,2)
//                            |       ,round(tue_sales1/tue_sales2,2)
//                            |       ,round(wed_sales1/wed_sales2,2)
//                            |       ,round(thu_sales1/thu_sales2,2)
//                            |       ,round(fri_sales1/fri_sales2,2)
//                            |       ,round(sat_sales1/sat_sales2,2)
//                            | FROM
//                            | (SELECT wswscs.d_week_seq d_week_seq1
//                            |        ,sun_sales sun_sales1
//                            |        ,mon_sales mon_sales1
//                            |        ,tue_sales tue_sales1
//                            |        ,wed_sales wed_sales1
//                            |        ,thu_sales thu_sales1
//                            |        ,fri_sales fri_sales1
//                            |        ,sat_sales sat_sales1
//                            |  FROM wswscs,date_dim
//                            |  WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001) y,
//                            | (SELECT wswscs.d_week_seq d_week_seq2
//                            |        ,sun_sales sun_sales2
//                            |        ,mon_sales mon_sales2
//                            |        ,tue_sales tue_sales2
//                            |        ,wed_sales wed_sales2
//                            |        ,thu_sales thu_sales2
//                            |        ,fri_sales fri_sales2
//                            |        ,sat_sales sat_sales2
//                            |  FROM wswscs, date_dim
//                            |  WHERE date_dim.d_week_seq = wswscs.d_week_seq AND d_year = 2001 + 1) z
//                            | WHERE d_week_seq1=d_week_seq2-53
//                            | ORDER BY d_week_seq1
//                          """.stripMargin)
//
//    val df3 = queryProvider.getDF("""
//                            | SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
//                            | FROM  date_dim dt, store_sales, item
//                            | WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
//                            |   AND store_sales.ss_item_sk = item.i_item_sk
//                            |   AND item.i_manufact_id = 128
//                            |   AND dt.d_moy=11
//                            | GROUP BY dt.d_year, item.i_brand, item.i_brand_id
//                            | ORDER BY dt.d_year, sum_agg desc, brand_id
//                            | LIMIT 100
//                          """.stripMargin)
//
//
//    val df7 = queryProvider.getDF("""
//                            | SELECT i_item_id,
//                            |        avg(ss_quantity) agg1,
//                            |        avg(ss_list_price) agg2,
//                            |        avg(ss_coupon_amt) agg3,
//                            |        avg(ss_sales_price) agg4
//                            | FROM store_sales, customer_demographics, date_dim, item, promotion
//                            | WHERE ss_sold_date_sk = d_date_sk AND
//                            |       ss_item_sk = i_item_sk AND
//                            |       ss_cdemo_sk = cd_demo_sk AND
//                            |       ss_promo_sk = p_promo_sk AND
//                            |       cd_gender = 'M' AND
//                            |       cd_marital_status = 'S' AND
//                            |       cd_education_status = 'College' AND
//                            |       (p_channel_email = 'N' or p_channel_event = 'N') AND
//                            |       d_year = 2000
//                            | GROUP BY i_item_id
//                            | ORDER BY i_item_id LIMIT 100
//                          """.stripMargin)
//
//    val df19 = queryProvider.getDF("""
//                             | select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
//                             | 	sum(ss_ext_sales_price) ext_price
//                             | from date_dim, store_sales, item,customer,customer_address,store
//                             | where d_date_sk = ss_sold_date_sk
//                             |   and ss_item_sk = i_item_sk
//                             |   and i_manager_id = 8
//                             |   and d_moy = 11
//                             |   and d_year = 1998
//                             |   and ss_customer_sk = c_customer_sk
//                             |   and c_current_addr_sk = ca_address_sk
//                             |   and substr(ca_zip,1,5) <> substr(s_zip,1,5)
//                             |   and ss_store_sk = s_store_sk
//                             | group by i_brand, i_brand_id, i_manufact_id, i_manufact
//                             | order by ext_price desc, brand, brand_id, i_manufact_id, i_manufact
//                             | limit 100
//                           """.stripMargin)

    val df3 = queryProvider.getDF("""
                            | SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand,SUM(ss_ext_sales_price) sum_agg
                            | FROM  date_dim dt, store_sales, item
                            | WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
                            |   AND store_sales.ss_item_sk = item.i_item_sk
                            |   AND item.i_manufact_id = 128
                            |   AND dt.d_moy=11
                            | GROUP BY dt.d_year, item.i_brand, item.i_brand_id
                            | ORDER BY dt.d_year, sum_agg desc, brand_id
                            | LIMIT 100
                          """.stripMargin)
    val df42 = queryProvider.getDF("""
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
                                   """.stripMargin)


//
//    val cost = CostEstimator.estimateCost(df3.queryExecution.optimizedPlan)
//    println(cost)

//    df3.write.save("/home/ntkhoa/output")

//    QueryExecutor.executeWorkSharing(strategyIndex, sqlc, Seq(df3, df42), outputDir)
//    while(true){
//      Thread.sleep(1000)
//    }

  }
}
