// We need to declare our optimizer in this package here due to the access restriction
package org.apache.spark.sql.extensions.optimizer

import java.math.BigInteger

import fr.eurecom.dsg.cost.CostConstants
import fr.eurecom.dsg.optimizer.{SimpleMCKPSolver, KnapsackItem, CEContainer, KnapsackClass}
import fr.eurecom.dsg.util.SparkSQLServerLogging
import nw.fr.eurecom.dsg.optimizer.{DFSVisitor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.extensions.Util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * This Optimizer optimizes a bag of Logical Plans (queries)
  * (currently supports only structured data sources which go with schemas, eg: json, parquet, csv, ...)
  *
  * Phases:
  * - Detects the sharing opportunities among the queries which are called Similar sub-Expressions (SEs)
  * - Build Covering Expression (CE) candidates for each group of SEs
  * which then might be cached in memory to speed up the query execution.
  * - Strategy Generation: Each strategy is a selection of zero or some cache plan(s).
  * - Strategy Selection phase: by estimating the cost of each individual strategy, produces the best strategy as the output.
  */
object CacheAwareOptimizer  extends SparkSQLServerLogging{

  def classifyCEs(CEContainers: ArrayBuffer[CEContainer]): Array[KnapsackClass] = {
    val res = new ArrayBuffer[KnapsackClass]()
    val partitionsByNumConsumers = CEContainers.groupBy(_.SEs.length)
    partitionsByNumConsumers.foreach(p1 =>{
      val partitionsByConsumers = p1._2.groupBy(_.SEs.map(_._2).mkString(" "))
      partitionsByConsumers.foreach(p2 =>{
        val c = new KnapsackClass()
        val subsets = p2._2.toSet.subsets
        subsets.foreach(s => {
          val S = s.toList
          if(S.length > 0){
            if(S.length == 1){
              val item = new KnapsackItem()
              item.profit = s.head.profit
              item.weight = s.head.weight
              item.content.append(s.head)
              c.addItem(item)
            }else{
              var keep = true

              for(i <- 0 until S.size)
                for(j<- 0 until S.size){
                  if(i != j && Util.containsDescendant(S(i).SEs(0)._1, S(j).SEs(0)._1))
                    keep = false
                }
              if(keep){
                val item = new KnapsackItem()
                for(i<- 0 until S.size){
                  item.profit = item.profit + S(i).profit
                  item.weight = item.weight+ S(i).weight
                  item.content.append(S(i))
                }
                c.addItem(item)
              }
            }
          }
        })

//        val ces = p2._2.sortBy(x => -Util.getNDescendants(x.CE))
//        val c = new KnapsackClass()
//        for(i <- 0 until ces.length){
//          val item = new KnapsackItem()
//          item.profit = ces(i).profit
//          item.weight = ces(i).weight
//          item.content.append(ces(i))
//          c.addItem(item)
//
//          for(j <- i+1 until ces.length){
//            if(!Util.containsDescendant(ces(i).SEs(0)._1, ces(j).SEs(0)._1)){
//              val lastItem = c.items.last
//
//              val item = new KnapsackItem()
//              item.profit = ces(i).profit + ces(j).profit
//              item.weight = ces(i).weight + ces(j).weight
//              item.content.append(ces(i))
//              item.content.append(ces(j))
//              c.addItem(item)
//
////              val item = new KnapsackItem()
////              item.profit = lastItem.profit + ces(j).profit
////              item.weight = lastItem.weight + ces(j).weight
////              item.content ++= lastItem.content
////              item.content.append(ces(j))
////              c.addItem(item)
//            }
//          }
//        }
        res.append(c)
      })
    })

    res.toArray
  }




  /**
    * Use the caching technique to globally optimize a given bag of (ìndividually) optimized Logical Plans (queries).
    * (currently, it supports only structured data sources which goes with the schema, eg: json, parquet, csv, ...)
    *
    * This is the main entry function of the Optimizer
 *
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
      for (relation <- Util.getLogicalRelations(inPlans(iPlan))){
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
    logInfo("Step 1: Identifying all SEs")
    val SEsMap = identifySEs(outputPlans)
    // common subexpressions are grouped by their table signature
    // HashMap<TableSignature, List<(CommonSubExpression, queryIndex)>>

    // ==============================================================
    // Step 2: Build CEs from the SEs
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 2: Building CEs")
    val CEContainers = new CoveringPlanBuilder().buildCoveringPlans(SEsMap)
    // SEs are grouped by a single CE
    // producer - consumer relationship

    // log how many CEs were built
    println("%d CEs were built".format(CEContainers.length))

    // now put in into classes and knapsack solver
    val knapsackClasses:Array[KnapsackClass] = classifyCEs(CEContainers)

    // log how many classes?
    println("Classes for the MCKP")
    knapsackClasses.foreach(c =>{
      println("class i has %d items".format(c.items.length))
    })

    val selectedItems = SimpleMCKPSolver.optimize(knapsackClasses, CostConstants.MAX_CACHE_SIZE)
    val selectedCEs = selectedItems.flatMap(kitem => kitem.content.asInstanceOf[ArrayBuffer[CEContainer]])

    // log selected items
    println("%d CEs were finally selected as cache plans".format(selectedCEs.length))
    selectedCEs.foreach(ce => println(ce.CE))

    // ==============================================================
    // Step 3: Strategy Generation
    // Generates all possible strategies.
    // Each strategy is a selection of zero or some cache plan(s)
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 3: Strategy Generation")
    new StrategyGenerator(outputPlans, selectedCEs)
    // We return this, the consumer can pull a strategy and judge its quality (step 4)
  }



  private def identifySEs(trees:Array[LogicalPlan])
  :mutable.HashMap[BigInteger, mutable.ArrayBuffer[(LogicalPlan, Int)]]={
    logInfo("Input of %d plan(s)".format(trees.length))

    // - Key: fingerprint/ signature
    // - Value: a set of (logical plan, tree index)
    // At least 2 logical plan(s) having the same signature is considered as a SE
    // We will produce one group of SE for each key that has the length(value) >= 2
    val fingerPrintMap = new mutable.HashMap[BigInteger, mutable.ArrayBuffer[(LogicalPlan, Int)]]
    def createNewList() = new mutable.ArrayBuffer[(LogicalPlan, Int)]()

    // Build a hash tree for each tree
    // key: plan, value: (fingerprint, plan's height)
    // We haven
    val hashTrees = new Array[mutable.HashMap[LogicalPlan, BigInteger]](trees.length)
    trees.indices.foreach {iPlan =>
      hashTrees(iPlan) = buildHashTree(trees(iPlan))
    }


    // Build fingerPrintMap
    trees.indices.foreach(i => {
      var isAllowedToMatchAfter = true // a flag variable
      val visitor = new DFSVisitor(trees(i))
      while (visitor.hasNext){
        val iPlan = visitor.getNext
        val iPlanFingerprint = hashTrees(i).get(iPlan).get

        val isFoundCommonSubtree = fingerPrintMap.contains(iPlanFingerprint)

        if(isAllowedToMatchAfter)
          fingerPrintMap.getOrElseUpdate(iPlanFingerprint, createNewList()).append(Tuple2(iPlan, i))

        if(isFoundCommonSubtree && !containCacheUnfriendlyOperator(iPlan)){
          isAllowedToMatchAfter = true
        }
        else
        {
          visitor.goDeeper() // keep looking (doesn't match, or containing cache-unfriendly operator)
          if(isFoundCommonSubtree && containCacheUnfriendlyOperator(iPlan)){
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

    // Re-scan one more time. Why? because of the case subexpression in subexpression.
    // TODO: explain more
    trees.indices.foreach(i => {
      val visitor = new DFSVisitor(trees(i))

      while (visitor.hasNext){
        val iPlan = visitor.getNext
        val iPlanFingerprint = hashTrees(i).get(iPlan).get

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
    println("Found %d group(s) of SEs".format(groupedCommonSubExpressions.size))
    groupedCommonSubExpressions.foreach(element => {
      println("fingerprint: %d, %d consumers".format(element._1, element._2.length))
      element._2.foreach(e => println(e._1.toString()))
    })

    groupedCommonSubExpressions
  }

  /**
    * Computes the hash value of a given LogicalPlan
    * The hash value is computed like the style of Hash Tree (Merkle Tree).
    * * ref: https://en.wikipedia.org/wiki/Merkle_tree
    * The hash value of a node is the hash of the "labels" of its children nodes.
    * This is a fast and secure way to search for common subtrees among many trees
    *
    * @param plan: node to compute the hash
    * @return (hash value of the given node)
    */
  def computeTreeHash(plan:LogicalPlan, hashTree:mutable.HashMap[LogicalPlan, BigInteger] = null):BigInteger={
    def updateHashTree(p:LogicalPlan, h:BigInteger): Unit ={
      if(hashTree != null)
        hashTree.put(p, h)
    }

    val className = plan.getClass.toString
    var hashVal:BigInteger = null

    plan match {

      // ================================================================
      // Binary Node case: `logicalPlan` has 2 children
      // ================================================================
      case b: BinaryNode =>
        var leftChildHash = computeTreeHash(b.left, hashTree)
        var rightChildHash = computeTreeHash(b.right, hashTree)
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
            // Consider only same join condition
            hashVal = Util.hash(className + b.joinType + b.condition + leftChildHash + rightChildHash)

          // override all signature, only consider identical expressions
          case b:Intersect =>
            hashVal = Util.hash(className + b.hashCode().toString)
          case b:Except =>
            hashVal = Util.hash(className + b.hashCode().toString)
          case _ => throw new IllegalArgumentException("not supported binary node")
        }

      case u:Union =>
        val childrenHash = u.children.map(c => computeTreeHash(c, hashTree)).mkString(" ")
        // Union should consider combining Filters, not Projects
        hashVal = Util.hash(className + childrenHash)

      // ================================================================
      // Unary Node case: `logicalPlan` has 1 child
      // ================================================================
      case u: UnaryNode =>
        val childHash = computeTreeHash(u.child, hashTree)
        u match{
          case u@(_:Filter | _:Project) => hashVal = Util.hash(className + childHash)

          case u@(_:GlobalLimit | _:LocalLimit | _:Sort | _:Aggregate) => // Only consider identical expression
            val childHash = Util.hash(u.hashCode().toString)
            hashVal = Util.hash(className + childHash)

          case _ =>
            val childHash = Util.hash(u.hashCode().toString)
            hashVal = Util.hash(className + childHash)
        }

      // ================================================================
      // Unary Node case: `logicalPlan` doesn't have any child
      // ================================================================
      case l: LeafNode => l match {
        case leaf:LogicalRelation =>
          val inputPath = Util.extractInputPath(leaf.relation)
          hashVal = Util.hash(className + inputPath)
        case _ => throw new IllegalArgumentException("unsupported leaf")
      }

      case _ => throw new IllegalArgumentException("unsupported logical plan")
    }

    updateHashTree(plan, hashVal)
    hashVal
  }

  private def buildHashTree(rootPlan:LogicalPlan):mutable.HashMap[LogicalPlan, BigInteger]={
    val res = new mutable.HashMap[LogicalPlan, BigInteger]()
    computeTreeHash(rootPlan, res)
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
 *
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

  def containCacheUnfriendlyOperator(plan:LogicalPlan): Boolean={
    plan.foreach(f => if(isUnfriendlyOperator(f)) return true)
    false
  }

  def isUnfriendlyOperator(plan:LogicalPlan): Boolean={
    plan match{
      case p @(_:Join | _:Union) => true
      case _ => false
    }
  }
}
