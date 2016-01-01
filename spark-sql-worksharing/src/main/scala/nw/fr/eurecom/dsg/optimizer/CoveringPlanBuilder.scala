package org.apache.spark.sql

import java.math.BigInteger
import org.apache.spark.sql.catalyst.expressions.{Or, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, Project, LogicalPlan}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class CoveringPlanBuilder {
  def buildCoveringPlans(commonSubExpressionsMap: mutable.HashMap[BigInteger, mutable.ListBuffer[(LogicalPlan, Int)]])
  :mutable.HashMap[LogicalPlan, Set[(LogicalPlan, Int)]]= {
    // Step 1: Prune bad plans
    // Step 2: Cluster plans using K-mean, distance is measure by the similarity metering
    // Step 3: Build and output a cache plan for each cluster, with the consumers are members belonging to that group

    val coveringExpressions= new mutable.HashMap[LogicalPlan, Set[(LogicalPlan, Int)]]
    // (covering plan, (original plan, consumer indexes))

    commonSubExpressionsMap.foreach(item =>{
      val plans = item._2.toArray
      // (plan, tree index)

      coveringExpressions.put(combinePlans(plans.map(ele => ele._1))._1, plans.toSet)
    })

    coveringExpressions
  }


  /**
    * combines logical plans to a common sharing plan
    * The LogicalPlans in plans must be the common subtrees in order to be able to be combined
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
    * @param planA: the first logical plan
    * @param planB: the second logical plan
    * @return (common sharing plan, isIdentical).
    *         isIdentical = true means the sharing plan is the same as planA and planB
    */
  def combinePlans(planA:LogicalPlan, planB:LogicalPlan): (LogicalPlan, Boolean) ={
    //assert(Util.isSameSubTree(planA, planB))

    if(planA.fastEquals(planB))// if they are identical, then return one of them as the covering plan
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
          var currentNode = plan
          while(currentNode.children.iterator.hasNext){
            val child = currentNode.children.iterator.next
            if(!child.isInstanceOf[LeafNode]){
              child.references.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item)
            }
            currentNode = child
          }
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
    }
  }
}
