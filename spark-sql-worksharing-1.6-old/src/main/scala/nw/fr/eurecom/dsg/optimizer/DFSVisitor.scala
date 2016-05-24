package nw.fr.eurecom.dsg.optimizer

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, UnaryNode}

import scala.collection.mutable


/**
 * Implement a modified version of Depth First Search for a Logical Plan tree where:
 * the User decides whether to:
 * - continue the visit, or
 * - skip visiting the current branch of the tree
 */
class DFSVisitor(val plan:LogicalPlan) {
  val root = plan
  var currentNode:LogicalPlan = root
  val stack = new mutable.Stack[LogicalPlan]()
  initialize()

  def initialize():Unit={
    stack.clear()
    stack.push(root)
  }

  def hasNext:Boolean={
    stack.nonEmpty
  }

  def reset():Unit={
    initialize()
  }

  def getNext:LogicalPlan={
    if(hasNext){
      currentNode = stack.pop()
      return currentNode
    }
    throw new Exception("no node to get!!!")
  }

  def goDeeper():Unit={
    currentNode match {
      case b:BinaryNode => b.children.foreach(stack.push)
      case u:UnaryNode => stack.push(u.child)
      case l:LeafNode =>
    }
  }
}
