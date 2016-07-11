package fr.eurecom.dsg.applications.microbenchmark.queries
import org.apache.spark.sql.{DataFrame}

class SimpleFiltering(data:DataFrame) extends MicroBQuery(data){
  val where1 = "(20 <= n1 and n1 <= 70)"
  val where2 = "(50 <= n1 and n1 <= 75)"

  val q1 = data.where(where1)
  val q2 = data.where(where2)
  override val cachePlan: DataFrame = data.where(where1 + " or " + where2)
  override val q1Opt: DataFrame = cachePlan.where(where1)
  override val q2Opt: DataFrame = cachePlan.where(where2)
}

// simple filtering with full cache
class SimpleFilteringFC(data:DataFrame) extends SimpleFiltering(data){
  override val cachePlan: DataFrame = data
}