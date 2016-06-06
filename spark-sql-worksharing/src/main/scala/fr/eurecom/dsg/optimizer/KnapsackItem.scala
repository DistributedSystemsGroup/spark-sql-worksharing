package fr.eurecom.dsg.optimizer

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by ntkhoa on 06/06/16.
  */
case class KnapsackItem(CE:LogicalPlan, SEs:Set[(LogicalPlan, Int)], profit:Double, weight:Double)