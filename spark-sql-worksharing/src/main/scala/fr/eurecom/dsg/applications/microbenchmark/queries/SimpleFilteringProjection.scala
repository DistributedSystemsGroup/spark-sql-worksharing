package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql._

class SimpleFilteringProjection (data:DataFrame) extends MicroBQuery(data) {
  def where1 = "(20 <= n1 and n1 <= 70)"
  def where2 = "(50 <= n1 and n1 <= 75)"

  def columns1 = Seq("n1", "n2", "n3", "n4", "n5", "d1", "d2", "d3", "d4", "d5", "s1", "s2", "s3", "s4", "s5")
  def columns2 = Seq("n1", "n2", "n3", "d1", "d2", "d3", "s1", "s2", "s3")
  def unionColumns = (columns1.toSet ++ columns2.toSet).toSeq // be aware of the fields in the where statements

  override def q1: DataFrame = data.where(where1).select(columns1.head, columns1.tail:_*)
  override def q2: DataFrame = data.where(where2).select(columns2.head, columns2.tail:_*)

  override def cachePlan: DataFrame = data.where(where1 + " or " + where2).select(unionColumns.head, unionColumns.tail:_*)

  override def q1Opt: DataFrame = cachePlan.where(where1).select(columns1.head, columns1.tail:_*)
  override def q2Opt: DataFrame = cachePlan.where(where2).select(columns2.head, columns2.tail:_*)
}

// simple filtering with full cache
class SimpleFilteringProjectionFC(data:DataFrame) extends SimpleFilteringProjection(data){
  override def cachePlan: DataFrame = data
}