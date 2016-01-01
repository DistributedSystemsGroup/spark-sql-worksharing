package nw.fr.eurecom.dsg.optimizer

import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.sql.CacheAwareOptimizer
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, UnaryNode, BinaryNode, LogicalPlan}


object Util {
  def hash(text:String): BigInteger=
    new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(text.getBytes()))

  /**
   * Important method for checking the equality of 2 subtrees
   * It uses the MerkleTree (Hash Tree) technique to compare 2 subtrees
   * @param planA: tree 1
   * @param planB: tree 2
   * @return
   */
  def isSameSubTree(planA:LogicalPlan, planB:LogicalPlan):Boolean=
    CacheAwareOptimizer.computeMerkleTreeHash(planA) == CacheAwareOptimizer.computeMerkleTreeHash(planB)

  def isIdenticalLogicalPlans(planA:LogicalPlan, planB:LogicalPlan):Boolean={
    planA.fastEquals(planB)
  }

  def containsChild(parentPlan:LogicalPlan, child:LogicalPlan): Boolean ={
    parentPlan.find(f => f.fastEquals(child)) match {
      case Some(item) => true
      case None => false
    }
  }

  def getHeight(plan:LogicalPlan):Int={
    plan match{
      case b:BinaryNode =>{
        val leftChildHeight = getHeight(b.left)
        val rightChildHeight = getHeight(b.right)
        math.max(leftChildHeight, rightChildHeight) + 1
      }
      case u:UnaryNode => getHeight(u.child) + 1
      case l: LeafNode => 1
    }
  }
}
