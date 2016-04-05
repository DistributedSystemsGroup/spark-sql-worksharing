// We need to declare our optimizer in this package here due to the access restriction
package org.apache.spark.sql.myExtensions.optimizer

import java.math.BigInteger
import com.databricks.spark.csv.CsvRelation
import nw.fr.eurecom.dsg.optimizer.{DFSVisitor, StrategyGenerator}
import nw.fr.eurecom.dsg.util.SparkSQLServerLogging
import org.apache.spark.sql.SQLContext
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
  * - Build covering expression candidates (cache plans candidates) for each group of common sub-trees
  * which then might be cached in memory to speed up the query execution.
  * - Strategy Generation: Each strategy is a selection of zero or some cache plan(s).
  * - Strategy Selection phase: by estimating the cost of each individual strategy, produces the best strategy as the output.
  */
object CacheAwareOptimizer  extends SparkSQLServerLogging{
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
    logInfo("========================================================")
    logInfo("Step 0: Pre-processing")
    val seenLogicalRelations = mutable.Set[LogicalRelation]()

    def getSeenRelation(relation:LogicalRelation):Option[LogicalRelation]={
      for (r <- seenLogicalRelations){
        if (Util.isSameRelation(r, relation))
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
    // Step 1: Identifying all common subexpressions
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 1: Identifying all common subexpressions")
    val commonSubExpressionsMap = identifyCommonSubExpressions(outputPlans)
    // common subexpressions are grouped by their table signature
    // HashMap<TableSignature, List<(CommonSubExpression, queryIndex)>>

    // ==============================================================
    // Step 2: Build covering expressions from the common subexpressions
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 2: Building covering expression candidates")
    val coveringExpressions = new CoveringPlanBuilder().buildCoveringPlans(commonSubExpressionsMap)
    // common subexpressions are grouped by a single covering expression
    // producer - consumer relationship
    // HashMap<CoveringExpression, List<(CommonSubExpression, queryIndex)>>

    // ==============================================================
    // Step 3: Strategy Generation
    // Generates all possible strategies.
    // Each strategy is a selection of zero or some cache plan(s)
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 3: Strategy Generation")
    new StrategyGenerator(outputPlans, coveringExpressions)
    // We return this, the consumer can pull a strategy and judge its quality (step 4)


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


  private def identifyCommonSubExpressions(trees:Array[LogicalPlan])
  :mutable.HashMap[BigInteger, mutable.ListBuffer[(LogicalPlan, Int)]]={

    // - Key: fingerprint/ signature
    // - Value: a set of (logical plan, tree index)
    // At least 2 logical plan(s) having the same signature is considered as a common subexpression
    // We will produce one common subexpression for each key that has the length(value) >= 2
    val fingerPrintMap = new mutable.HashMap[BigInteger, mutable.ListBuffer[(LogicalPlan, Int)]]
    def createNewList() = new mutable.ListBuffer[(LogicalPlan, Int)]()

    // Build a hash tree for each tree
    // key: plan, value: (fingerprint, plan's height)
    // We haven
    val hashTrees = new Array[mutable.HashMap[LogicalPlan, (BigInteger, Int)]](trees.length)
    trees.indices.foreach {iPlan =>
      hashTrees(iPlan) = buildHashTree(trees(iPlan))
    }

    logInfo("Input of %d plan(s)".format(trees.length))
    trees.foreach(t => log.info(t.toString()))

    // Build fingerPrintMap
    trees.indices.foreach(i => {
      logInfo("Checking tree %d".format(i))
      var isAllowedToMatchAfter = true // a super variable
      val visitor = new DFSVisitor(trees(i))
      while (visitor.hasNext){
        var continue = true // used to break to following while loop
        val iPlan = visitor.getNext
        logInfo("Checking operator %s".format(iPlan.getClass.getSimpleName))
        val iPlanFingerprint = hashTrees(i).get(iPlan).get._1

        val foundCommonSubtree = fingerPrintMap.contains(iPlanFingerprint)

        if(isAllowedToMatchAfter)
          fingerPrintMap.getOrElseUpdate(iPlanFingerprint, createNewList()).append(Tuple2(iPlan, i))

        if(foundCommonSubtree && !containCacheUnfriendlyOperator(iPlan)){
          logInfo("At tree %d: Found a match for: \n%s".format(i,iPlan.toString()))
          logInfo("Stopped the find on this branch")
          isAllowedToMatchAfter = true
        }
        else
        {
          logInfo("Keep finding on this branch")
          visitor.goDeeper() // keep looking (doesn't match, or containing cache-unfriendly operator)
          if(foundCommonSubtree && containCacheUnfriendlyOperator(iPlan)){
            if(isAllowedToMatchAfter)
              logInfo("At tree %d: Found a match for \n%s".format(i,iPlan.toString()))
            else
              logInfo("Found a match but we already have a previous better solution")

            if(isUnfriendlyOperator(iPlan)){
              isAllowedToMatchAfter = true // re-enable
            }
            else{
              isAllowedToMatchAfter = false
            }
          }
        }
      }
    })

    logInfo("Rescanning again to detect expression in expression sharing")
    // Re-scan one more time. Why? because of the case subexpression in subexpression.
    // TODO: explain more
    trees.indices.foreach(i => {
      logInfo("Checking tree %d".format(i))
      val visitor = new DFSVisitor(trees(i))

      while (visitor.hasNext){
        val iPlan = visitor.getNext
        logInfo("Checking operator %s".format(iPlan.getClass.getSimpleName))
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
    val groupedCommonSubExpressions = fingerPrintMap.filter(keyvaluePair => keyvaluePair._2.size >= 2)
    logInfo("========================================================")
    logInfo("Found %d group(s) of common subexpressions".format(groupedCommonSubExpressions.size))
    groupedCommonSubExpressions.foreach(element => {
      logInfo("fingerprint: %d, %d consumers".format(element._1, element._2.length))
      element._2.foreach(e => println(e._1))
    })

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
            case _ => throw new IllegalArgumentException("notsupported binary node")
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
            case u@(_:Filter | _:Project) => hashVal = Util.hash(className + childHash)

            case u@(_:Limit | _:Sort | _:Aggregate) => // Only consider identical expression
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
            // We are using structured format input files with schema (json, csv, parquet).
            // Thus, if the 2 relations are read from the same file location then they are the same.
            leaf.relation match {
              case r:HadoopFsRelation=> r.paths.foreach(paths += _.trim) // ParquetRelation or JSONRelation
              case r:CsvRelation => paths += r.location.get
              case _ => throw new IllegalArgumentException("unknown relation")
            }
            hashVal = Util.hash(className + paths)
            val ret = (hashVal, 1)
            res.put(l, ret)
            ret
          case _ => throw new IllegalArgumentException("notsupported leaf")
        }

        case _ => throw new IllegalArgumentException("notsupported logical plan")
      }
    }

    computeTreeHash(rootPlan)
    res
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

  def isUnfriendlyOperator(plan:LogicalPlan):Boolean={
    plan match{
      case p:Join => true
      case p:Union => true
      case _ => false
    }
  }
}
