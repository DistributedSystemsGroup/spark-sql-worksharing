package fr.eurecom.dsg.applications.microbenchmark.queries

import org.apache.spark.sql.{DataFrame}

/**
  * Created by ntkhoa on 30/06/16.
  */
abstract class MicroBQuery(data:DataFrame) {
  val q1:DataFrame
  val q2:DataFrame

  val cachePlan:DataFrame
  val q1Opt:DataFrame
  val q2Opt:DataFrame

  def warmUp():Unit = {
    data.foreach(_ => ())
  }

  def runWithoutOpt(): Unit ={
    warmUp()
    q1.foreach(_ => ())
    q2.foreach(_ => ())
  }

  def runWithOpt():Unit = {
    warmUp()
    cachePlan.cache()
    q1Opt.foreach(_ => ())
    q2Opt.foreach(_ => ())
  }
}
