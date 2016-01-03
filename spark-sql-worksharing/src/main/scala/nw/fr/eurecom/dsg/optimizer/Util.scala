package org.apache.spark.sql.myExtensions.optimizer

import java.math.BigInteger
import java.security.MessageDigest
import com.databricks.spark.csv.CsvRelation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.HadoopFsRelation


object Util {
  def hash(text:String): BigInteger=
    new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(text.getBytes()))

  /**
   * Important method for checking the equality of 2 subtrees
   * It uses the MerkleTree (Hash Tree) technique to compare 2 subtrees
   * @param relationA: tree 1
   * @param relationB: tree 2
   * @return
   */
  def isSameRelation(relationA:LogicalRelation, relationB:LogicalRelation):Boolean={
    var pathsA = ""
    val classA = relationA.getClass.toString
    relationA.relation match {
      case r:HadoopFsRelation=> r.paths.foreach(pathsA += _.trim) // ParquetRelation or JSONRelation
      case r:CsvRelation => pathsA += r.location.get
      case _ => case _ => throw new IllegalArgumentException("unknown relation")
    }
    val hashA = Util.hash(classA + pathsA)

    var pathsB = ""
    val classB = relationB.getClass.toString
    relationB.relation match {
      case r:HadoopFsRelation=> r.paths.foreach(pathsB += _.trim) // ParquetRelation or JSONRelation
      case r:CsvRelation => pathsB += r.location.get
      case _ => case _ => throw new IllegalArgumentException("unknown relation")
    }
    val hashB = Util.hash(classB + pathsB)

    hashA == hashB
  }



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


  /**
    * External version of CacheAwareOptimizer.computeTreeHash
    * @param plan: node to compute the hash
    * @return (hash value of the given node)
    */
  def computeTreeHashExternal(plan:LogicalPlan):BigInteger={
    val className = plan.getClass.toString
    var hashVal:BigInteger = null
    plan match {
      // ================================================================
      // Binary Node case: `logicalPlan` has 2 children
      // ================================================================
      case b: BinaryNode =>
        var leftChildHash = computeTreeHashExternal(b.left)
        var rightChildHash = computeTreeHashExternal(b.right)
        // Unifying the order of left & right child before computing the hash of the parent node
        // Do sorting such that the leftChildHash should always lower or equals the rightChildHash
        // We want (A Join B) to be the same as (B Join A)
        if(leftChildHash.compareTo(rightChildHash) > 0){
          // Do the swap
          val tmpSwap = leftChildHash
          leftChildHash = rightChildHash
          rightChildHash = tmpSwap
        }
        b match {
          case b:Join =>
            // Join should consider combing Filters and Projects
            hashVal = Util.hash(className + b.joinType + b.condition + leftChildHash + rightChildHash)
          case b:Union =>
            // Union should consider combining Filters, not Projects
            hashVal = Util.hash(className + leftChildHash + rightChildHash)
          case b:Intersect =>
            hashVal = Util.hash(className + b.hashCode().toString)
          case b:Except =>
            hashVal = Util.hash(className + b.hashCode().toString)
          case _ => throw new IllegalArgumentException("unknown binary node")
        }
        hashVal

      // ================================================================
      // Unary Node case: `logicalPlan` has 1 child
      // ================================================================
      case u: UnaryNode =>
        val childHash = computeTreeHashExternal(u.child)
        u match{
          case u@(_:Filter | _:Project) => hashVal = Util.hash(className + childHash)

          case u@(_:Limit | _:Sort | _:Aggregate) => // Only consider identical expression
            val childHash = Util.hash(u.hashCode().toString)
            hashVal = Util.hash(className + childHash)

          case _ =>
            val childHash = Util.hash(u.hashCode().toString)
            hashVal = Util.hash(className + childHash)
        }
        hashVal

      // ================================================================
      // Unary Node case: `logicalPlan` doesn't have any child
      // ================================================================
      case l: LeafNode => l match {
        case leaf:LogicalRelation =>
          var paths = ""
          // We are using structured format input files with schema (json, csv, parquet).
          // Thus, if the 2 relations are read from the same file location then they are the same.
          leaf.relation match {
            case r:HadoopFsRelation=> r.paths.foreach(paths += _.trim) // ParquetRelation or JSONRelation
            case r:CsvRelation => paths += r.location.get
            case _ => case _ => throw new IllegalArgumentException("unknown relation")
          }
          hashVal = Util.hash(className + paths)
          hashVal
        case _ => throw new IllegalArgumentException("illegal leaf")
      }

      case _ => throw new IllegalArgumentException("illegal logical plan")
    }
  }

}
