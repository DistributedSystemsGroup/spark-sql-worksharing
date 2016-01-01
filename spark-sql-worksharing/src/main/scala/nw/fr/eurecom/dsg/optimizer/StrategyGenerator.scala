package nw.fr.eurecom.dsg.optimizer

import org.apache.spark.sql.CacheAwareOptimizer
import org.apache.spark.sql.catalyst.expressions.{And, Or, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  *
  * @param inPlans
  * @param coveringExpressions: HashMap(covering plan, Set(original plan, consumer indexes))
  */
class StrategyGenerator (inPlans:Array[LogicalPlan],
                         coveringExpressions:mutable.HashMap[LogicalPlan, Set[(LogicalPlan, Int)]]){

  val rewrittenPlans = new Array[LogicalPlan](inPlans.length)
  inPlans.copyToArray(rewrittenPlans)

  // GENERATE the power set of cachingPlans
  val subsets = coveringExpressions.toSet.subsets

  def hasNext():Boolean = subsets.hasNext
  def next():Strategy = {
    val selectedCoveringExpressions = subsets.next().toArray.sortBy(ele => Util.getHeight(ele._1) * -1)
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

  def buildExtractionPlan(plan:LogicalPlan, coveringPlan:LogicalPlan):LogicalPlan={
    if(!plan.isInstanceOf[BinaryNode])
      throw new Exception("expected binary node " + plan.toString())
    val root = plan.asInstanceOf[BinaryNode]

    def extract(left:LogicalPlan, right:LogicalPlan): LogicalPlan ={
      (left, right) match{
        case (a@Project(projectListA, _), b@Project(projectListB, _)) => {
          val combinedProjectList = ArrayBuffer[NamedExpression]()
          projectListA.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item)
          projectListB.foreach(item => if(!combinedProjectList.contains(item)) combinedProjectList += item)
          val child = extract(a.child, b.child)
          Project(combinedProjectList, child)
        }
        case (a@Filter(conditionA, _), b@Filter(conditionB, _)) => {
          val child = extract(a.child, b.child)

          if(conditionA.fastEquals(conditionB)){
            Filter(conditionA, child)
          }
          else{
            Filter(And(conditionA, conditionB), child)
          }
        }
        case _ => coveringPlan
      }

    }
    extract(root.left, root.right)
  }



}
