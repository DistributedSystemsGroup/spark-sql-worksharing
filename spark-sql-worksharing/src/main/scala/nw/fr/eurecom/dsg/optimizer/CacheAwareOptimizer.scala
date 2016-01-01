// We need to declare our optimizer in this package here due to the access restriction
package org.apache.spark.sql

import java.math.BigInteger
import com.databricks.spark.csv.CsvRelation
import nw.fr.eurecom.dsg.optimizer.{DFSVisitor, StrategyGenerator, Util}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.HadoopFsRelation
import scala.collection.mutable

/**
  * This Optimizer optimizes a bag of Logical Plans (queries)
  * (currently supports only structured data sources which go with schemas, eg: json, parquet, csv, ...)
  *
  * Phases:
  * - Detects the sharing opportunities among the queries which are called "common sub-expressions"
  * - Transforms the common sub-trees into cache plans which then might be cached in memory to speed up the query execution.
  * - Strategy Generation: Each strategy is a selection of some cache plans.
  * - Strategy Selection phase: by estimating the cost of each individual strategy, produces the best strategy as the output.
  */
object CacheAwareOptimizer {
  /**
    * Use the caching technique to globally optimize a given bag of (ìndividually) optimized Logical Plans (queries).
    * (currently, it supports only structured data sources which goes with the schema, eg: json, parquet, csv, ...)
    *
    * This is the main entry function of the Optimizer
    * @param inPlans: array of queries. To be more exact, array of (individually) Optimized LogicalPlans
    * @param sqlContext: SQLContext
    * @return the best (globally optimized) CacheAwareStrategy to be executed
    */
  def optimizePlans(inPlans:Array[LogicalPlan], sqlContext:SQLContext):StrategyGenerator={
    val outputPlans = new Array[LogicalPlan](inPlans.length)
    inPlans.copyToArray(outputPlans)

    // ==============================================================
    // Step 0: Pre-processing:
    // Resolving the AttributeReference#ID problem. Refer to method
    // `transformToSameDatasource` for more information
    // ==============================================================

    val seenLogicalRelations = mutable.Set[LogicalRelation]()

    def getSeenRelation(relation:LogicalRelation):Option[LogicalRelation]={
      for (r <- seenLogicalRelations){
        if (Util.isSameSubTree(r, relation))
          return Some(r)
      }
      None
    }

    inPlans.indices.foreach { iPlan =>
      for (relation <- getLogicalRelations(inPlans(iPlan))){
        getSeenRelation(relation) match {
          case Some(standardRelation:LogicalRelation) => outputPlans(iPlan) = transformToSameDatasource(standardRelation, relation, outputPlans(iPlan))
          case None => seenLogicalRelations.add(relation)
        }
      }
    }

    // ==============================================================
    // Step 1: Identifying all common sub-trees
    // ==============================================================
    val commonSubExpressionsMap = identifyCommonSubExpressions(outputPlans)

    // ==============================================================
    // Step 2: Build covering expressions from the common subexpressions
    // ==============================================================
    val coveringExpressions = new CoveringPlanBuilder().buildCoveringPlans(commonSubExpressionsMap)

    // ==============================================================
    // Step 3: Strategy Generation
    // Generates all possible strategies.
    // Each strategy is a selection of some cache plans.
    // ==============================================================
    new StrategyGenerator(outputPlans, coveringExpressions)

    // ==============================================================
    // Step 4: Strategy Selection
    // estimates the cost of each individual strategy, produces
    // the best strategy as the output.
    // ==============================================================

//    var bestStrategy = generator.next()
//    while(generator.hasNext()){
//    val strategy = generator.next()
//     comparing the cost
//    }
//    bestStrategy
//    val dataFrames = bestStrategy.execute(sqlContext)
  }

  def identifyCommonSubExpressions(trees:Array[LogicalPlan])
  :mutable.HashMap[BigInteger, mutable.ListBuffer[(LogicalPlan, Int)]]={

    // - Key: fingerprint
    // - Value: a set of (logical plan, tree index)
    // The MultiMap allows us to use the addBinding method for quickly adding
    // We will produce one common subexpression for each key that has the length(value) >= 2
    val fingerPrintMap = new mutable.HashMap[BigInteger, mutable.ListBuffer[(LogicalPlan, Int)]]
    def createNewList = new mutable.ListBuffer[(LogicalPlan, Int)]()

    // Build a hash tree for each tree
    // key: plan, value: (fingerprint, plan's height)
    // We haven
    val hashTrees = new Array[mutable.HashMap[LogicalPlan, (BigInteger, Int)]](trees.length)
    trees.indices.foreach {iPlan =>
      hashTrees(iPlan) = buildHashTree(trees(iPlan))
    }

    // Build fingerPrintMap
    trees.indices.foreach(i => {
      val visitor = new DFSVisitor(trees(i))
      while (visitor.hasNext){
        var foundBestSubtree = false
        var continue = true // used to break to following while loop
        val iPlan = visitor.getNext
        val iPlanFingerprint = hashTrees(i).get(iPlan).get._1

        foundBestSubtree = fingerPrintMap.contains(iPlanFingerprint)

        // check this, cache-friendly & cache-unfriendly operators
        if(!foundBestSubtree || containCacheUnfriendlyOperator(iPlan)){
          visitor.goDeeper()
        }
        fingerPrintMap.getOrElseUpdate(iPlanFingerprint, createNewList).append(Tuple2(iPlan, i))
      }
    })

    // Re-scan one more time. Why? because of the case subexpression in subexpression.
    // TODO: explain more
    trees.indices.foreach(i => {
      val visitor = new DFSVisitor(trees(i))
      while (visitor.hasNext){
        val iPlan = visitor.getNext
        val iPlanFingerprint = hashTrees(i).get(iPlan).get._1

        val found = (fingerPrintMap.contains(iPlanFingerprint)
          && fingerPrintMap.get(iPlanFingerprint).get.size >= 2)
        visitor.goDeeper()
        if(found){
          if(!fingerPrintMap.get(iPlanFingerprint).get.contains(Tuple2(iPlan, i))){
            fingerPrintMap.getOrElseUpdate(iPlanFingerprint, createNewList).append(Tuple2(iPlan, i))
          }
        }
      }
    })

    // Keep only those keys that have the length(value) >= 2 (common subtrees)
    val groupedCommonSubExpressions = fingerPrintMap.filter(keyvaluePair => keyvaluePair._2.size > 1)
    groupedCommonSubExpressions
  }

  def buildHashTree(rootPlan:LogicalPlan):mutable.HashMap[LogicalPlan, (BigInteger, Int)]={
    val res = new mutable.HashMap[LogicalPlan, (BigInteger, Int)]()

    /**
      * Computes the hash value of a given LogicalPlan
      * The hash value is computed like the style of Hash Tree (Merkle Tree).
      * * ref: https://en.wikipedia.org/wiki/Merkle_tree
      * The hash value of a node is the hash of the "labels" of its children nodes.
      * This is a fast and secure way to search for common subtrees among many trees
      *
      * @param plan: node to compute the hash
      * @return (hash value of the given node, height)
      */
    def computeTreeHash(plan:LogicalPlan):(BigInteger, Int)={
      val className = plan.getClass.toString
      var hashVal:BigInteger = null
      plan match {
        // ================================================================
        // Binary Node case: `logicalPlan` has 2 children
        // ================================================================
        case b: BinaryNode =>
          var (leftChildHash, leftChildHeight) = computeTreeHash(b.left)
          var (rightChildHash, rightChildHeight) = computeTreeHash(b.right)
          // Unifying the order of left & right child before computing the hash of the parent node
          // Do sorting such that the leftChildHash should always lower or equals the rightChildHash
          // We want (A Join B) to be the same as (B Join A)
          if(leftChildHash.compareTo(rightChildHash) > 0){
            // Do the swap
            val tmpSwap = leftChildHash
            leftChildHash = rightChildHash
            rightChildHash = tmpSwap

            val tmp2Swap = leftChildHeight
            leftChildHeight = rightChildHeight
            rightChildHeight = tmp2Swap
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
          val ret = (hashVal, math.max(leftChildHeight, rightChildHeight) + 1)
          res.put(b, ret)
          ret

        // ================================================================
        // Unary Node case: `logicalPlan` has 1 child
        // ================================================================
        case u: UnaryNode =>
          val (childHash, childHeight) = computeTreeHash(u.child)
          u match{
            case u:Filter =>
              hashVal = Util.hash(className + childHash)
            case u:Project =>
              hashVal = Util.hash(className + childHash)
            //        case u:Limit =>
            //          val childHash = computeMerkleTreeHash(u.child)
            //          Util.hash(className + childHash.toString())
            case u:Sort =>
              //hashVal = Util.hash(className + u.order + u.global + childHash) // Only consider identical expression
              val childHash = Util.hash(u.hashCode().toString)
              hashVal = Util.hash(className + childHash)
            case u:Aggregate => // Only consider identical expression
              val childHash = Util.hash(u.hashCode().toString)
              hashVal = Util.hash(className + childHash)
            case _ =>
              val childHash = Util.hash(u.hashCode().toString)
              hashVal = Util.hash(className + childHash)
          }
          val ret = (hashVal, childHeight + 1)
          res.put(u, ret)
          ret

        // ================================================================
        // Unary Node case: `logicalPlan` doesn't have any child
        // ================================================================
        case l: LeafNode => l match {
          case leaf:LogicalRelation =>
            var paths = ""
            // We are using structured format input files with schema (csv, parquet).
            // Thus, if the 2 relations are read from the same file location then they are the same.
            leaf.relation match {
              case r:HadoopFsRelation=> r.paths.foreach(paths += _.trim) // ParquetRelation or JSONRelation
              case r:CsvRelation => paths += r.location.get
              case _ => case _ => throw new IllegalArgumentException("unknown relation")
            }
            hashVal = Util.hash(className + paths)
            val ret = (hashVal, 1)
            res.put(l, ret)
            ret
          case _ => throw new IllegalArgumentException("illegal leaf")
        }

        case _ => throw new IllegalArgumentException("illegal logical plan")
      }
    }

    computeTreeHash(rootPlan)
    res
  }

  /**
    * Computes the hash value of a given LogicalPlan
    * The hash value is computed like the style of Hash Tree (Merkle Tree).
    * * ref: https://en.wikipedia.org/wiki/Merkle_tree
    * The hash value of a node is the hash of the "labels" of its children nodes.
    * This is a fast and secure way to search for common subtrees among many trees
    *
    * @param logicalPlan: node to compute the hash
    * @return Hash value of the given node
    */
  def computeMerkleTreeHash(logicalPlan:LogicalPlan):BigInteger={
    val className = logicalPlan.getClass.toString
    logicalPlan match {
      // ================================================================
      // Binary Node case: `logicalPlan` has 2 children
      // ================================================================
      case b: BinaryNode =>
        var leftChildHash = computeMerkleTreeHash(b.left)
        var rightChildHash = computeMerkleTreeHash(b.right)
        // Do sorting such that the leftChildHash should always lower or equals the rightChildHash
        // We want (A Join B) to be the same as (B Join A)
        if(leftChildHash.compareTo(rightChildHash) > 0){
          val tmp = leftChildHash
          leftChildHash = rightChildHash
          rightChildHash = tmp
        }
        b match{
          case b:Join =>
            Util.hash(className + b.joinType + b.condition + leftChildHash + rightChildHash)
          case b:Union =>
            Util.hash(className + leftChildHash + rightChildHash)
          case b:Intersect =>
            Util.hash(className + leftChildHash + rightChildHash)
          case b:Except =>
            Util.hash(className + leftChildHash + rightChildHash)
          case _ => throw new IllegalArgumentException("unknown binary node " + b.getClass)
        }

      // ================================================================
      // Unary Node case: `logicalPlan` has 1 child
      // ================================================================
      case u: UnaryNode =>
        val childHash = computeMerkleTreeHash(u.child)
        u match{
          case u:Filter =>
            Util.hash(className + childHash)
          case u:Project =>
            Util.hash(className + childHash)
          //        case u:Limit =>
          //          val childHash = computeMerkleTreeHash(u.child)
          //          Util.hash(className + childHash.toString())
          case u:Sort =>
            Util.hash(className + u.order + u.global + childHash)
          case _ =>
            val childHash = Util.hash(logicalPlan.hashCode().toString())
            Util.hash(className + childHash)
        }

      // ================================================================
      // Unary Node case: `logicalPlan` doesn't have any child
      // ================================================================
      case l: LeafNode => l match {
        case leaf:LogicalRelation =>
          var paths = ""
          // We are using structured format input files with schema (csv, parquet).
          // Thus, if the 2 relations are read from the same file location then they are the same.
          leaf.relation match {
            case r:HadoopFsRelation=> r.paths.foreach(paths += _.trim) // ParquetRelation or JSONRelation
            case r:CsvRelation => paths += r.location.get
            case _ => case _ => throw new IllegalArgumentException("unknown relation")
          }
          Util.hash(className + paths)
        case _ => throw new IllegalArgumentException("illegal leaf")
      }

      case _ => throw new IllegalArgumentException("illegal logical oldPlan")
    }
  }

  /**
    * PROBLEM: AttributeReference#ID
    * Some terms:
    * - AttributeReference: A reference to an attribute produced by another operator in the tree.
    * - The ID of an AttributeReference: a globally unique id for a given named expression, used to identify which
    * attribute output by a relation is being referenced in a subsequent computation.
    *
    * Because the queries (LogicalPlans) are initialized by each user (in their JVMs), these IDs are no longer unique.
    * This method will be used to resolve that problem
    * Given a replaceByRelation and tobeReplacedRelation which are actually the same relation (because the AttributeReference#IDs are not the same)
    * Replaces the tobeReplacedRelation (which is the child of "oldPlan") by replaceByRelation and transforms upwards the attribute reference#ID
    * @param replaceByRelation
    * @param tobeReplacedRelation
    * @param oldPlan
    * @return
    */
  private def transformToSameDatasource(replaceByRelation:LogicalRelation, tobeReplacedRelation:LogicalRelation, oldPlan:LogicalPlan):LogicalPlan={
    val standardOutputs = replaceByRelation.output.toList
    val currentOutputs = tobeReplacedRelation.output.toList

    def getCorrespondingAR(a:AttributeReference): AttributeReference ={
      for(item <- standardOutputs){
        if(item.name == a.name && currentOutputs.contains(a))
          return item.asInstanceOf[AttributeReference]
      }
      a
    }
    // replace the tobeReplacedRelation in oldPlan by replaceByRelation
    var outPlan = oldPlan.transform({
      case `tobeReplacedRelation` => replaceByRelation
    })

    // transform all attribute references of outPlan having the same name as those of outPlan
    // this is because of the differences in attribute ID between 2 logical plans
    // solve the AttributeReference#ID problem
    // TODO: A bug here, need to track the ID, the getCorrespondingAR only matchs by name
    outPlan = outPlan.transformUp({
      case p => p.transformExpressions({
        case a:AttributeReference => getCorrespondingAR(a)
      })
    })

    outPlan
  }

  /**
    * Find all LogicalRelation nodes of a plan
    * The leaf nodes of a logical plan are very usually Logical Relations - loading up the data
    * @param plan: the logical plan (query)
    * @return sequence of LogicalRelations
    */
  def getLogicalRelations(plan:LogicalPlan):Seq[LogicalRelation]={
    plan.collect{
      case n:LogicalRelation => n
    }
  }

  def containCacheUnfriendlyOperator(plan:LogicalPlan):Boolean={
    plan.foreach {
      case p: Join => return true
      case p: Union => return true
      case _ =>
    }
    false
  }

}
