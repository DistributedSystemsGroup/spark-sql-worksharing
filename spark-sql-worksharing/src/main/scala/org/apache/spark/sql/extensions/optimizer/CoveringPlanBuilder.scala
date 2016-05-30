package org.apache.spark.sql.extensions.optimizer

import java.math.BigInteger

import fr.eurecom.dsg.cost.CostConstants
import fr.eurecom.dsg.util.SparkSQLServerLogging
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Or}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, Filter, LogicalPlan, Project, UnaryNode}
import org.apache.spark.sql.extensions.cost.CostEstimator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.extensions.Util

class CoveringPlanBuilder extends SparkSQLServerLogging{

  /**
    *
    * @param commonSubExpressionsMap
    * @return
    */
  def buildCoveringPlans(commonSubExpressionsMap: mutable.HashMap[BigInteger, mutable.ListBuffer[(LogicalPlan, Int)]])
  :mutable.HashMap[LogicalPlan, Set[(LogicalPlan, Int)]]= {
    // For each group
    // Step 1: keep only good candidates (prune bad candidates)
    // Good candidate:
    // - expensive to run (otherwise, re-run from scratch may be better)
    //    + Disk I/O heavy expression, determined by: big #in and small #out/#in fraction
    //    OR
    //    + CPU intensive & Network I/O heavy expression: contains join, sort, aggregate, but #out is fit in memory
    // - has high number of consumers (to be reused many times) (to be adjusted)
    //
    // Heuristics:
    // 1. Prune small profit SE group
    // 2. Prune SEs having big output
    commonSubExpressionsMap.foreach(groupI =>{
      logInfo("Handling group %d".format(groupI._1))
      val peerPlans = groupI._2.map(ele => ele._1)
      val nConsumers = peerPlans.length

      val estimates = peerPlans.map(p => CostEstimator.estimateCost(p))
      val totalExecCosts = estimates.map(e => e.getExecutionCost).sum
      val minEstimate = estimates.minBy(e => e.getExecutionCost)

      val maximumSaving  = totalExecCosts -
        (minEstimate.getExecutionCost
        + CostEstimator.estimateMaterializingCost(minEstimate.getOutputSize)
        + CostEstimator.estimateRetrievingCost(minEstimate.getOutputSize) * nConsumers)

      if(maximumSaving < CostConstants.MIN_SAVING){
        println("pruned group " + groupI)
        // dont share this group of SE
      }else{
        for(i <- 0 until(nConsumers)){
          println(peerPlans(i))
          println(estimates(i))
        }
        println("Maximum savving = %s".format(maximumSaving))

        for(i <- 0 until(nConsumers)){
          if (estimates(i).getOutputSize > CostConstants.MAX_CACHE_SIZE){

          }
        }


      }



    // Step 2: for each group, build a covering expression that covers all its consumers
    // TODO: explain why cover all is enough?
    // Tradeoff: wider subexpression can serves more #consumers, but its' output will be large
    // leading to high materializing (to RAM) cost
    //




    // Step 3: output the result




      // (plan, tree index)


      // Good candidates:



      //val coveringExp = combinePlans(peerPlans.map(ele => ele._1))._1
    })

    // Step 3: Build and output a cache plan for each cluster, with the consumers are members belonging to that group
    val coveringExpressions= new mutable.HashMap[LogicalPlan, Set[(LogicalPlan, Int)]]
    // (covering plan, (original plan, consumer indexes))

    commonSubExpressionsMap.foreach(item =>{
      val plans = item._2.toArray
      // (plan, tree index)

      val coveringExp = combinePlans(plans.map(ele => ele._1))._1
      logInfo("Built covering expression for %d:\n%s".format(item._1, coveringExp))
      coveringExpressions.put(coveringExp, plans.toSet)
    })

    coveringExpressions
  }


  /**
    * combines logical plans to a common sharing plan
    * The LogicalPlans in plans must be the common subtrees in order to be able to be combined
 *
    * @param plans: array of logical plans
    * @return (common sharing plan, isIdentical).
    *         isIdentical = true means the sharing plan is the same as all plan in plans
    */
  def combinePlans(plans: Array[LogicalPlan]): (LogicalPlan, Boolean) ={
    assert(plans.length >= 2)
    var identical = true
    var combinedPlan:LogicalPlan = plans(0)
    (1 to plans.length - 1).foreach { i =>
      val (cPlan, isIdentical) = combinePlans(combinedPlan, plans(i))
      if(!isIdentical)
        identical = false
      combinedPlan = cPlan
    }
    (combinedPlan, identical)
  }

  /**
    * combines 2 logical plans to a common sharing plan
    * This is a recursive method.
    * planA and planB must be the common subtrees in order to be able to be combined
 *
    * @param planA: the first logical plan
    * @param planB: the second logical plan
    * @return (common sharing plan, isIdentical).
    *         isIdentical = true means the sharing plan is the same as planA and planB
    */
  def combinePlans(planA:LogicalPlan, planB:LogicalPlan): (LogicalPlan, Boolean) ={

    if(Util.isIdenticalLogicalPlans(planA, planB))// if they are identical, then return one of them as the covering plan
      return (planA, true)

    (planA, planB) match{
      case (a@Project(projectListA, _), b@Project(projectListB, _)) => {
        // =================================================================================
        // PROJECT($"colA", $"colB")
        // PROJECT($"colA", $"colC", $"colD")
        // will be combined to PROJECT($"colA", $"colB", $"colC", $"colD")
        // Remember to add projection columns that are required by the child plans,
        // because we will reuse the common sharing plan in the future.
        // =================================================================================

        val combinedProjectList = ArrayBuffer[NamedExpression]()
        projectListA.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item)
        projectListB.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item)

        def addRequiredProjectRefs(plan:LogicalPlan): Unit ={
          val filtersOps = plan.collect{case n:Filter => n}.toArray
          filtersOps.foreach(f => f.references.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item))
        }

        addRequiredProjectRefs(a)
        addRequiredProjectRefs(b)

        val (combinedChild, _) = combinePlans(a.child, b.child)
        (Project(combinedProjectList, combinedChild), false)
      }

      // =================================================================================
      // FILTER(conditionA)
      // FILTER(conditionB)
      // if conditionA != conditionB
      //  FILTER(OR(conditionA, conditionB))
      // if conditionA == conditionB
      //  FILTER(conditionA)
      // =================================================================================
      case (a@Filter(conditionA, _), b@Filter(conditionB, _)) => {
        val (combinedChild, _) = combinePlans(a.child, b.child)

        if(conditionA.fastEquals(conditionB)){
          (Filter(conditionA, combinedChild), false)
        }
        else{
          (Filter(Or(conditionA, conditionB), combinedChild), false)
        }
      }

      // =================================================================================
      // a and b must be identical
      // a & b could be: Sort, Limit, Aggregate
      // Just return a or b
      // in fact, the first check of this function `if(planA.fastEquals(planB))` already did the job!
      // =================================================================================
      case (a:UnaryNode, b:UnaryNode) =>{
        assert(a.getClass == b.getClass)
        (a, true)
      }

      // =================================================================================
      // JOIN(leftA, rightA, joinType, condition)
      // JOIN(leftB, rightB, joinType, condition)
      //
      // check the `computeTreeHash` function for more information.
      // joinType & condition must be the same between 2 JOINs
      // 2 cases might happen!!!
      // leftA ~ leftB and rightA ~ rightB
      // leftA ~ rightB and rightA ~ leftB
      // =================================================================================
      case (a:BinaryNode, b:BinaryNode) =>{
        if(Util.isSameHash(a.left, b.left)){
          //leftA ~ leftB and rightA ~ rightB
          val (leftCombined, _) = combinePlans(a.left, b.left)
          val (rightCombined, _) = combinePlans(a.right, b.right)
          (a.withNewChildren(Array(leftCombined, rightCombined)), false)

        }else{
          //leftA ~ rightB and rightA ~ leftB
          val (leftCombined, _) = combinePlans(a.left, b.right)
          val (rightCombined, _) = combinePlans(a.right, b.left)
          (a.withNewChildren(Array(leftCombined, rightCombined)), false)
        }
      }

      case _ => throw new IllegalArgumentException("Wrong match")
    }
  }
}
