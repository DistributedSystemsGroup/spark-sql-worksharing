package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql._

/**
  * Created by ntkhoa on 30/06/16.
  */
class SimpleProjection (data:DataFrame) extends MicroBQuery(data){
  def columns1 = Seq("n1", "n2", "n3", "n4", "n5", "d1", "d2", "d3", "d4", "d5", "s1", "s2", "s3", "s4", "s5")
//  def columns2 = Seq("n1", "n2", "n3", "d1", "d2", "d3", "s1", "s2", "s3")
  def columns2 = Seq("n1", "n2", "n3", "d5", "d6", "d7", "s5", "s6", "s7")
  def unionColumns = (columns1.toSet ++ columns2.toSet).toSeq

  override def q1: DataFrame = data.select(columns1.head, columns1.tail:_*)
  override def q2: DataFrame = data.select(columns2.head, columns2.tail:_*)

  override def cachePlan: DataFrame = data.select(unionColumns.head, unionColumns.tail:_*)
  override def q1Opt: DataFrame = cachePlan.select(columns1.head, columns1.tail:_*)
  override def q2Opt: DataFrame = cachePlan.select(columns2.head, columns2.tail:_*)
}


// simple projection with full cache
class SimpleProjectionFC(data:DataFrame) extends SimpleProjection(data){
  override def cachePlan: DataFrame = data
}