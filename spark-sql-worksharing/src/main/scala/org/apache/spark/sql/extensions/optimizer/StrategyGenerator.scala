package org.apache.spark.sql.extensions.optimizer

import fr.eurecom.dsg.optimizer.KnapsackItem
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, And}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.extensions.Util

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**
  * Created by ntkhoa on 06/06/16.
  */
class StrategyGenerator(inPlans: Array[LogicalPlan], val CEs: ArrayBuffer[KnapsackItem]) {


  def get(i:Int):Strategy = {
    val rewrittenPlans = new Array[LogicalPlan](inPlans.length)
    inPlans.copyToArray(rewrittenPlans)
    val selectedCoveringExpression = (CEs(i).CE, CEs(i).SEs.toArray.sortBy(ele => Util.getHeight(ele._1) * -1))
    val selectedCoveringExpressions = new ArrayBuffer[(LogicalPlan, Array[(LogicalPlan, Int)])]()
    selectedCoveringExpressions.append(selectedCoveringExpression)
    // sort it to solve the expression in expression problem

    val cachePlans = new ListBuffer[LogicalPlan]()
    selectedCoveringExpressions.indices.foreach { i =>
      val coveringExpression = selectedCoveringExpressions(i)._1
      val consumers = selectedCoveringExpressions(i)._2
      cachePlans.append(coveringExpression)
      consumers.foreach(consumer => {
        val originalPlan = consumer._1
        val consumerIndex = consumer._2

        if(!originalPlan.fastEquals(coveringExpression)){
          if(CacheAwareOptimizer.containCacheUnfriendlyOperator(coveringExpression)){
            // extractPlan = ANDING filters & projects from originalPlan
            // Thay originalPlan = extractPlan on top of coveringPlan
            val extractPlan = buildExtractionPlan(originalPlan, coveringExpression)
            rewrittenPlans(consumer._2) = rewrittenPlans(consumer._2).transform{
              case `originalPlan` => extractPlan
            }
          }
          else{
            rewrittenPlans(consumer._2) = rewrittenPlans(consumer._2).transform{
              case `originalPlan` => originalPlan.transformUp{
                case x:LeafNode => coveringExpression
              }
            }

            // also remember to transform on the previous cache plans
            cachePlans.indices.foreach(iCachePlan => {
              if(iCachePlan < i){
                cachePlans(iCachePlan) = cachePlans(iCachePlan).transformUp{
                  case `originalPlan` => coveringExpression
                }
              }
            })

          }

        }
        else{
          // we don't have to rewrite
        }
      })


    }

    new Strategy(rewrittenPlans, cachePlans.toArray)
  }

  def getTopProjections(plan:LogicalPlan):Array[Project]={
    plan match{
      case u:UnaryNode => u match{
        case p:Project => Array(p)
        case _ => getTopProjections(u.child)
      }
      case b:BinaryNode =>{
        val l1 = getTopProjections(b.left)
        val l2 = getTopProjections(b.right)
        l1 ++ l2
      }
      case l:LeafNode => null
    }
  }

  def buildExtractionPlan(plan:LogicalPlan, coveringPlan:LogicalPlan):LogicalPlan={
    // collect all filters to "anding" the predicates
    // collect all "top" projections and "merge" them
    // project(filter(coveringPlan))

    val filteringOps = plan.collect{case n:Filter => n}.toArray

    val projectionOps = getTopProjections(plan)

    var andingAllFilterPredicates = filteringOps(0).condition
    for(i <- 1 to filteringOps.length - 1){
      andingAllFilterPredicates = new And(andingAllFilterPredicates, filteringOps(i).condition)
    }

    val combinedProjectList = ArrayBuffer[NamedExpression]()
    projectionOps.foreach(proj =>
      proj.projectList.foreach(
        item => if(!combinedProjectList.contains(item))
          combinedProjectList += item))

    Project(combinedProjectList, Filter(andingAllFilterPredicates, coveringPlan))
    //    if(!plan.isInstanceOf[BinaryNode])
    //      throw new Exception("expected binary node " + plan.toString())
    //    val root = plan.asInstanceOf[BinaryNode]
    //
    //    def extract(left:LogicalPlan, right:LogicalPlan): LogicalPlan ={
    //      (left, right) match{
    //        case (a@Project(projectListA, _), b@Project(projectListB, _)) => {
    //          val combinedProjectList = ArrayBuffer[NamedExpression]()
    //          projectListA.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item)
    //          projectListB.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item)
    //          val child = extract(a.child, b.child)
    //          Project(combinedProjectList, child)
    //        }
    //        case (a@Filter(conditionA, _), b@Filter(conditionB, _)) => {
    //          val child = extract(a.child, b.child)
    //
    //          if(conditionA.fastEquals(conditionB)){
    //            Filter(conditionA, child)
    //          }
    //          else{
    //            Filter(And(conditionA, conditionB), child)
    //          }
    //        }
    //        case _ => coveringPlan
    //      }
    //
    //    }
    //
    //
    //
    //    extract(root.left, root.right)
  }
}
