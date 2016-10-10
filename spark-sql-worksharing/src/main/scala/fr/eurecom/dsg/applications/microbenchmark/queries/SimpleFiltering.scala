package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql.DataFrame

class SimpleFiltering(data: DataFrame) extends MicroBenchmarkExperiment(data) {
  def where1 = "(20 <= n1 and n1 <= 70)"

  def where2 = "(50 <= n1 and n1 <= 75)"

  def query1 = data.where(where1)

  def query2 = data.where(where2)

  override def cachePlan: DataFrame = data.where(where1 + " or " + where2)

  override def query1WS: DataFrame = cachePlan.where(where1)

  override def query2WS: DataFrame = cachePlan.where(where2)
}

/**
  * Alternative version of SimpleFiltering where the cache plan is the base relation
  */
class SimpleFilteringFC(data: DataFrame) extends SimpleFiltering(data) {
  override def cachePlan: DataFrame = data
}