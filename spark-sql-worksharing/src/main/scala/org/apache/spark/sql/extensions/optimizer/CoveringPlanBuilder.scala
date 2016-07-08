package org.apache.spark.sql.extensions.optimizer

import java.math.BigInteger

import fr.eurecom.dsg.cost.CostConstants
import fr.eurecom.dsg.optimizer.{CEContainer}
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
    * @param SEGroups
    * @return
    */
  def buildCoveringPlans(SEGroups: mutable.HashMap[BigInteger, mutable.ArrayBuffer[(LogicalPlan, Int)]])
  :ArrayBuffer[CEContainer]= {
    val res = new ArrayBuffer[CEContainer]()

    SEGroups.foreach(groupI => {
      // For each group of SEs
      logInfo("Handling group %d".format(groupI._1))
      val SEs = groupI._2
      var nConsumers = SEs.length

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

      // obtain the estimates (cost, input, output, ...) for each SE using CostEstimator
      val estimates = SEs.map(p => CostEstimator.estimateCost(p._1))

      for (i <- 0 until (nConsumers)) {
        println(SEs(i))
        println(estimates(i))
      }

      // compute total execution cost for this group of SEs
      val totalExecCost = estimates.map(e => e.getExecutionCost).sum
      // get the smallest execution cost among the SEs in this group
      val minEstimate = estimates.minBy(e => e.getExecutionCost)

      val maximumSaving = totalExecCost -
        (minEstimate.getExecutionCost
          + CostEstimator.estimateMaterializingCost(minEstimate.getOutputSize)
          + CostEstimator.estimateRetrievingCost(minEstimate.getOutputSize) * nConsumers)

      println("Maximum savving = %s".format(maximumSaving))

      if (maximumSaving < CostConstants.MIN_SAVING) {
        println("pruned this whole group of SEs: " + groupI)
        // dont share this group of SE (dont build a CE for this group)
      } else {
        // Prune SEs having big output
        var executionCostWithoutOpt:Double = 0

        val filteredSEs = SEs.zipWithIndex.filter {
          case ((p: LogicalPlan, id: Int), i: Int) => {
            if (estimates(i).getOutputSize <= CostConstants.MAX_CACHE_SIZE){
              executionCostWithoutOpt += estimates(i).getExecutionCost
              true
            }
            else {
              println("Removed SE due to big output: " + estimates(i).getOutputSize)
              println(p)
              false
            }
          }
        }.map(_._1)
        if(filteredSEs.length >= 2){
          nConsumers = filteredSEs.length
          // now, build a CE for the filtered SEs. Then wrap it in the Knapsack item
          val (ce, identical) = combinePlans(filteredSEs.map(ele => ele._1).toArray)
          val ceEstimate = CostEstimator.estimateCost(ce)
          val executingCECost = ceEstimate.getExecutionCost
          val materializingCost = CostEstimator.estimateMaterializingCost(ceEstimate.getOutputSize)
          val retrievingCost = CostEstimator.estimateRetrievingCost(ceEstimate.getOutputSize) * nConsumers
          val COMPCosts = 0 //TODO: compute COMPCosts
          val executionCostWithOpt = executingCECost + materializingCost + retrievingCost + COMPCosts
          val profit = executionCostWithoutOpt - executionCostWithOpt
          if(profit > 0){
            val weight = ceEstimate.getOutputSize
            res.append(new CEContainer(ce, filteredSEs, profit, weight))
            println("built a CE " + ce)
            println("profit = " + profit)
            println("weight = " + weight)
          }
          else{
            println("warning, profit < 0")
          }
        }
        else{
          println("removed group due to there is less than 2 consumers")
        }
      }
    })

    res
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
      case (a@Project(projectListA, _), b@Project(projectListB, _)) =>
        // =================================================================================
        // PROJECT($"colA", $"colB")
        // PROJECT($"colA", $"colC", $"colD")
        // will be combined to PROJECT($"colA", $"colB", $"colC", $"colD")
        // Remember to add projection columns that are required by the child plans,
        // because we will reuse the common sharing plan in the future.
        // =================================================================================

        val combinedProjectList = ArrayBuffer[NamedExpression]()
        val addedIDs = ArrayBuffer[Long]()

        def update(item:NamedExpression): Unit ={
          if(!addedIDs.contains(item.exprId.id)) {
            combinedProjectList += item
            addedIDs.append(item.exprId.id)
          }
        }

        projectListA.foreach(update)
        projectListB.foreach(update)


//        def addRequiredProjectRefs(plan:LogicalPlan): Unit ={
//          val filtersOps = plan.collect{case n:Filter => n}.toArray
//          filtersOps.foreach(f => f.references.foreach(update))
//        }
//
//        addRequiredProjectRefs(a)
//        addRequiredProjectRefs(b)

        val filtersOpsA = a.collect{case fNode:Filter => fNode}.toList
        val filtersOpsB = b.collect{case fNode:Filter => fNode}.toList

        val symmetricDifference = new ArrayBuffer[Filter]()
        (filtersOpsA++filtersOpsB).foreach(f =>{
          val idx = symmetricDifference.indexOf(f)
          if(idx != -1)
            symmetricDifference.remove(idx)
          else
            symmetricDifference.append(f)
          }
        )

        symmetricDifference.foreach(f => f.references.foreach(update))

        val (combinedChild, _) = combinePlans(a.child, b.child)
        (Project(combinedProjectList, combinedChild), false)

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
