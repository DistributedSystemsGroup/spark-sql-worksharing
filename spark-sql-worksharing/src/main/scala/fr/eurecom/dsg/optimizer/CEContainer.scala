package fr.eurecom.dsg.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import scala.collection.mutable.ArrayBuffer

case class CEContainer(CE:LogicalPlan, SEs:ArrayBuffer[(LogicalPlan, Int)], profit:Double, weight:Double)



