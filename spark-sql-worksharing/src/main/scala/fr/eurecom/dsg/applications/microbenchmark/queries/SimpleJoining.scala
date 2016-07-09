package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql.DataFrame

class SimpleJoining(data:DataFrame) extends MicroBQuery(data){

  val whereLeft1 = "(20 <= n1 and n1 <= 70)"
  val whereLeft2 = "(50 <= n1 and n1 <= 75)"
  val columnsLeft1 = Seq("n1", "n3", "d1", "d3", "s1", "s3")
  val columnsLeft2 = Seq("n1", "n2", "d1", "d2", "s1", "s2")

  val columnsRight = Seq("i1", "f1", "str1")
  val whereRight = "(50 <= i1 and i1 <= 60)"

  override val q1: DataFrame = data.where(whereLeft1).select(columnsLeft1.head, columnsLeft1.tail:_*).join(data, "n1 = i1")
  override val q2: DataFrame = data.where(whereLeft2).select(columnsLeft2.head, columnsLeft2.tail:_*)

  override val cachePlan: DataFrame = data.where(whereLeft1 + " or " + whereLeft2)
  override val q1Opt: DataFrame = ???
  override val q2Opt: DataFrame = ???
}
