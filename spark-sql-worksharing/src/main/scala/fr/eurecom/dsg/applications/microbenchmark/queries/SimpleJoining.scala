package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql.DataFrame

class SimpleJoining(data:DataFrame, refData:DataFrame) extends MicroBQuery(data){

  val whereLeft1 = "(20 <= n1 and n1 <= 70)"
  val whereLeft2 = "(50 <= n1 and n1 <= 75)"
  val whereRight1 = "(ref_n1 <= 200000)"
  val whereRight2 = "(ref_n1 <= 200000)"

  val columnsLeft1 = Seq("n1", "n4", "d1", "d2", "s1", "s2")
  val columnsLeft2 = Seq("n1", "n4", "d1", "d5", "s1", "s5")
  val unionLeftColumns = (columnsLeft1.toSet ++ columnsLeft2.toSet).toSeq

  val columnsRight1 = Seq("ref_n1", "ref_n2", "ref_d1", "ref_d2", "ref_s1", "ref_s2")
  val columnsRight2 = Seq("ref_n1", "ref_n2", "ref_d1", "ref_d2", "ref_s1", "ref_s2")
  val unionRightColumns = (columnsRight1.toSet ++ columnsRight2.toSet).toSeq

  val extract1 = (columnsLeft1.toSet ++ columnsRight1.toSet).toSeq
  val extract2 = (columnsLeft2.toSet ++ columnsRight2.toSet).toSeq

  override def warmUp():Unit = {
    data.foreach(_ => ())
    refData.foreach(_ => ())
  }

  override def q1: DataFrame = data.where(whereLeft1).select(columnsLeft1.head, columnsLeft1.tail:_*).join(
    refData.where(whereRight1).select(columnsRight1.head, columnsRight1.tail:_*),
    data("n4") === refData("ref_n1"))
  override def q2: DataFrame = data.where(whereLeft2).select(columnsLeft2.head, columnsLeft2.tail:_*).join(
    refData.where(whereRight2).select(columnsRight2.head, columnsRight2.tail:_*),
    data("n4") === refData("ref_n1"))

  override def cachePlan: DataFrame = data.where(whereLeft1 + " or " + whereLeft2).select(unionLeftColumns.head, unionLeftColumns.tail:_*).join(
    refData.where(whereRight1 + " or " + whereRight2).select(unionRightColumns.head, unionRightColumns.tail:_*),
    data("n4") === refData("ref_n1"))
  override def q1Opt: DataFrame = cachePlan.where(whereLeft1 + " and " + whereRight1).select(extract1.head, extract1.tail:_*)
  override def q2Opt: DataFrame = cachePlan.where(whereLeft2 + " and " + whereRight2).select(extract2.head, extract2.tail:_*)


//  override def runWithOpt():Unit = {
//    warmUp()
//    cachePlan.count() // run this to get the cardinality information
//    cachePlan.cache()
//    q1Opt.foreach(_ => ())
//    q2Opt.foreach(_ => ())
//  }
//
//  override def runWithoutOpt(): Unit ={
//    warmUp()
//    q1.count()
//    q2.count()
//    q1.foreach(_ => ())
//    q2.foreach(_ => ())
//  }
}
