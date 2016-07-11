package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql.DataFrame

class SimpleJoining(data:DataFrame) extends MicroBQuery(data){

  def whereLeft1 = "(20 <= n1 and n1 <= 70)"
  def whereLeft2 = "(50 <= n1 and n1 <= 75)"
  def columnsLeft1 = Seq("n1", "n3", "d1", "d3", "s1", "s3")
  def columnsLeft2 = Seq("n1", "n2", "d1", "d2", "s1", "s2")

  def columnsRight = Seq("i1", "f1", "str1")
  def whereRight = "(50 <= i1 and i1 <= 60)"

  override def q1: DataFrame = data.where(whereLeft1).select(columnsLeft1.head, columnsLeft1.tail:_*).join(data, "n1 = i1")
  override def q2: DataFrame = data.where(whereLeft2).select(columnsLeft2.head, columnsLeft2.tail:_*)

  override def cachePlan: DataFrame = data.where(whereLeft1 + " or " + whereLeft2)
  override def q1Opt: DataFrame = ???
  override def q2Opt: DataFrame = ???
}
