package fr.eurecom.dsg.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ntkhoa on 06/06/16.
  */
case class CEContainer(CE:LogicalPlan, SEs:ArrayBuffer[(LogicalPlan, Int)], profit:Double, weight:Double)



