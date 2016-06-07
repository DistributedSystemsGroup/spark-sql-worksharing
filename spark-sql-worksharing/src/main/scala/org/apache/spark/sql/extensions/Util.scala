package org.apache.spark.sql.extensions

import java.io.InvalidObjectException
import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{LogicalRelation, HadoopFsRelation}
import org.apache.spark.sql.extensions.optimizer.CacheAwareOptimizer
import org.apache.spark.sql.sources.BaseRelation

object Util {
  /**
    * Create new DataFrame from a LogicalPlan
    * @param sparkSession
    * @param logicalPlan
    * @return
    */
  def toDataFrame(sparkSession: SparkSession, logicalPlan:LogicalPlan): DataFrame = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  def stringToInt(key:String, mod:Int):Int = {
    val res = key.hashCode() % mod
    if(res < 0)
      res + mod
    else
      res
  }

  def extractTableName(r:BaseRelation): String = {
    r match {
      case r:HadoopFsRelation => r.location.paths.iterator.next.getName
      case _ => throw new InvalidObjectException("cannot extract table name from relation" + r.toString)
    }
  }

  def extractInputPath(r:BaseRelation): String = {
    r match {
      case r:HadoopFsRelation => r.location.paths.iterator.next.toString
      case _ => throw new InvalidObjectException("cannot extract table name from relation" + r.toString)
    }
  }

  /**
    * Find all LogicalRelation nodes of a plan
    * The leaf nodes of a logical plan are very usually Logical Relations - loading up the data
    *
    * @param plan: the logical plan (query)
    * @return sequence of LogicalRelations
    */
  def getLogicalRelations(plan:LogicalPlan):Seq[LogicalRelation]={
    plan.collect{
      case n:LogicalRelation => n
    }
  }


  def hash(text:String): BigInteger = new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(text.getBytes()))

  /**
    * Important method for checking the equality of 2 subtrees
    * It uses the MerkleTree (Hash Tree) technique to compare 2 subtrees
    * @param relationA: tree 1
    * @param relationB: tree 2
    * @return
    */
  def isSameRelation(relationA:LogicalRelation, relationB:LogicalRelation):Boolean={
    val pathsA = extractInputPath(relationA.relation)
    val classA = relationA.getClass.toString
    val hashA = Util.hash(classA + pathsA)

    val pathsB = extractInputPath(relationB.relation)
    val classB = relationB.getClass.toString
    val hashB = Util.hash(classB + pathsB)

    hashA == hashB
  }

  def isIdenticalLogicalPlans(planA:LogicalPlan, planB:LogicalPlan):Boolean={
    planA.fastEquals(planB)
  }

  def isSameHash(planA:LogicalPlan, planB:LogicalPlan):Boolean={
    CacheAwareOptimizer.computeTreeHash(planA) == CacheAwareOptimizer.computeTreeHash(planB)
  }

  def containsDescendant(parentPlan:LogicalPlan, child:LogicalPlan): Boolean ={
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

  def getNDescendants(plan:LogicalPlan):Int ={
    plan match{
      case b:BinaryNode =>{
        val lDescendants = getNDescendants(b.left)
        val rDescendants = getNDescendants(b.right)
        lDescendants + rDescendants + 1
      }
      case u:UnaryNode => getNDescendants(u.child) + 1
      case l: LeafNode => 1
    }
  }
}
