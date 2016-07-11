package fr.eurecom.dsg.applications.microbenchmark.queries
import org.apache.spark.sql.{DataFrame}

class SimpleFiltering(data:DataFrame) extends MicroBQuery(data){
  def where1 = "(20 <= n1 and n1 <= 70)"
  def where2 = "(50 <= n1 and n1 <= 75)"

  def q1 = data.where(where1)
  def q2 = data.where(where2)
  override def cachePlan: DataFrame = data.where(where1 + " or " + where2)
  override def q1Opt: DataFrame = cachePlan.where(where1)
  override def q2Opt: DataFrame = cachePlan.where(where2)
}

// simple filtering with full cache
class SimpleFilteringFC(data:DataFrame) extends SimpleFiltering(data){
  override def cachePlan: DataFrame = data
}