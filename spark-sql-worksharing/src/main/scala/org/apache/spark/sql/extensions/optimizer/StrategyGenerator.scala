package org.apache.spark.sql.extensions.optimizer

import fr.eurecom.dsg.optimizer.CEContainer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.extensions.Util

import scala.collection.mutable.{ListBuffer, ArrayBuffer}

class StrategyGenerator(inPlans: Array[LogicalPlan], val CEs: Array[CEContainer]) {
  def getWeight(): Double = CEs.map(ce => ce.weight).sum

  def getProfit(): Double = CEs.map(ce => ce.profit).sum


  def get():Strategy = {
    val rewrittenPlans = new Array[LogicalPlan](inPlans.length)
    inPlans.copyToArray(rewrittenPlans)
    // sort it to solve the expression in expression problem
    val selectedCoveringExpressions = CEs.map(ce => (ce.CE, ce.SEs.toArray.sortBy(ele => -Util.getHeight(ele._1))))

    val cachePlans = new ListBuffer[LogicalPlan]()
    selectedCoveringExpressions.indices.foreach { i =>
      val coveringExpression = selectedCoveringExpressions(i)._1
      val consumers = selectedCoveringExpressions(i)._2
      cachePlans.append(coveringExpression)
      consumers.foreach(consumer => {
        val originalPlan = consumer._1
        val consumerIndex = consumer._2

        if(!originalPlan.fastEquals(coveringExpression)){
          if(CacheAwareOptimizer.containsCacheUnfriendlyOperator(coveringExpression)){
            // extractPlan = ANDING filters & projects from originalPlan
            // Thay originalPlan = extractPlan
            val extractPlan = buildExtractionPlan(originalPlan, coveringExpression)
            rewrittenPlans(consumerIndex) = rewrittenPlans(consumerIndex).transform{
              case `originalPlan` => extractPlan
            }
          }
          else{
            val extractPlan = buildExtractionPlan(originalPlan, coveringExpression)
            rewrittenPlans(consumerIndex) = rewrittenPlans(consumerIndex).transform{
              case `originalPlan` => extractPlan
            }
//            rewrittenPlans(consumerIndex) = rewrittenPlans(consumerIndex).transform{
//              case `originalPlan` => originalPlan.transformUp{
//                case x:LeafNode => coveringExpression
//              }
//            }

          }

        }
        else{
          // we don't have to rewrite
        }
      })


    }

    new Strategy(rewrittenPlans, cachePlans.toArray)
  }

  private def getTopProjections(plan:LogicalPlan):Array[Project]={
    plan match{
      case u:UnaryNode => u match{
        case p:Project => Array(p)
        case _ => getTopProjections(u.child)
      }
      case b:BinaryNode =>{
        val l1 = getTopProjections(b.left)
        val l2 = getTopProjections(b.right)
        (l1,l2) match {
          case (null, null) => null
          case (_, null) => l1
          case (null, _) => l2
          case (_, _) => l1 ++ l2
        }
      }
      case l:LeafNode => null
    }
  }

  private def buildExtractionPlan(plan:LogicalPlan, coveringPlan:LogicalPlan):LogicalPlan={
    // collect all filters to "anding" the predicates
    // collect all "top" projections and "merge" them
    // project(filter(coveringPlan))

    val filters = plan.collect{case n:Filter => n}
    val projectionOps = getTopProjections(plan)
    var filteringOps:Array[Filter] = null

    var andingAllFilterPredicates:Expression = null
    if(filters != null) {
      filteringOps = filters.filter(f => !Util.containsDescendant(coveringPlan, f)).toArray
      if (filteringOps.nonEmpty) {
        andingAllFilterPredicates = filteringOps(0).condition
        for (i <- 1 to filteringOps.length - 1) {
          andingAllFilterPredicates = new And(andingAllFilterPredicates, filteringOps(i).condition)
        }
      }
    }

    var combinedProjectList:ArrayBuffer[NamedExpression] = null

    if(projectionOps != null){
      combinedProjectList = ArrayBuffer[NamedExpression]()
      projectionOps
        .filter(p => !Util.containsDescendant(coveringPlan, p))
        .foreach(proj =>
        proj.projectList.foreach(
          item => if(!combinedProjectList.contains(item))
            combinedProjectList += item))

      // resolve the alias problem
      combinedProjectList = combinedProjectList.map(ex => ex match{
        case a:Alias =>
          val child = unwrapCast(a.child)
          child match{
            case ar:AttributeReference => ar.withName(a.name).withExprId(a.exprId)
            case _ => ex
          }
        case _ => ex
      })


    }

    if(andingAllFilterPredicates != null && combinedProjectList != null)
      Project(combinedProjectList, Filter(andingAllFilterPredicates, coveringPlan))
    else{
      if(andingAllFilterPredicates == null)
        Project(combinedProjectList, coveringPlan)
      else if(combinedProjectList == null)
        Filter(andingAllFilterPredicates, coveringPlan)
      else
        coveringPlan
    }
  }

  private def unwrapCast(e:Expression): Expression ={
    e match {
      case e:Cast => e.child
      case _ => e
    }
  }
}
