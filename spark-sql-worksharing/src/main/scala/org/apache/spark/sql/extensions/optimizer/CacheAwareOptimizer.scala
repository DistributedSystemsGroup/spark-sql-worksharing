// We need to declare our optimizer in this package here due to the access restriction
package org.apache.spark.sql.extensions.optimizer

import java.math.BigInteger

import fr.eurecom.dsg.cost.CostConstants
import fr.eurecom.dsg.optimizer.{CEContainer, ItemClass, ItemImpl, MCKnapsackSolverDynamicProgramming}
import fr.eurecom.dsg.util.SparkSQLServerLogging
import nw.fr.eurecom.dsg.optimizer.DFSVisitor
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
  * which then might be cached in memory to speed up the query execution. Each CE is assigned with (profit, weight)
  * - Solve the multiple choice knapsack problem (MCKP)
  * - Rewriting the original queries
  */
object CacheAwareOptimizer extends SparkSQLServerLogging {

  /**
    * Use the caching technique to globally optimize a given bag of (individually) optimized Logical Plans (queries).
    * (currently, it supports only structured data sources which goes with the schema, eg: json, parquet, csv, ...)
    *
    * This is the main entry function of the Optimizer
    *
    * @param inPlans : array of queries. To be exact, it is the array of (individually) Optimized LogicalPlans
    * @return the best (globally optimized) StrategyGenerator to be executed
    */
  def optimizeWithWorkSharing(inPlans: Array[LogicalPlan]): StrategyGenerator = {
    val beginning = System.nanoTime()

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

    def getSeenRelation(relation: LogicalRelation): Option[LogicalRelation] = {
      for (r <- seenLogicalRelations) {
        if (!r.fastEquals(relation) && Util.isSameRelation(r, relation))
          return Some(r)
      }
      None
    }

    inPlans.indices.foreach { iPlan =>
      for (relation <- Util.getLogicalRelations(inPlans(iPlan))) {
        getSeenRelation(relation) match {
          case Some(standardRelation: LogicalRelation) => outputPlans(iPlan) = transformToSameDatasource(standardRelation, relation, outputPlans(iPlan))
          case None => seenLogicalRelations.add(relation)
        }
      }
    }

    logInfo("Until step 0 elapsed: %f".format((System.nanoTime() - beginning) / 1e9))

    // ==============================================================
    // Step 1: Identifying all common subexpressions
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 1: Identifying all SEs")
    val SEsMap = identifySEs(outputPlans)
    // SEs are grouped by their signature
    // HashMap<SESignature, ArrayBuffer<(SE, ConsumerQueryIndex)>>
    logInfo("Until step 1 elapsed: %f".format((System.nanoTime() - beginning) / 1e9))

    // ==============================================================
    // Step 2: Build CEs from the SEs
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 2: Building CEs")
    val CEContainers = new CoveringPlanBuilder().buildCoveringPlans(SEsMap)
    // SEs are grouped by a single CE
    // producer - consumer relationship

    // log how many CEs were built
    logInfo("%d CEs were built".format(CEContainers.length))

    // now put in into classes and knapsack solver
    val itemClassesForMCKP: Array[ItemClass] = classifyCEs(CEContainers)

    // log how many classes?
    logInfo("Classes for the MCKP")
    itemClassesForMCKP.foreach(c => {
      logInfo("class i has %d items".format(c.items.length))
    })

    logInfo("Until step 2.0 elapsed: %f".format((System.nanoTime() - beginning) / 1e9))

    val selectedItems = MCKnapsackSolverDynamicProgramming.solve(CostConstants.MAX_CACHE_SIZE_GB, itemClassesForMCKP)
    val selectedCEs = selectedItems.flatMap(kitem => kitem.asInstanceOf[ItemImpl].tag.asInstanceOf[ArrayBuffer[CEContainer]])

    // log selected items
    logInfo("%d CEs were finally selected as cache plans".format(selectedCEs.length))
    selectedCEs.foreach(ce => logInfo("\n" + ce.CE.toString()))

    logInfo("Until step 2.5 elapsed: %f".format((System.nanoTime() - beginning) / 1e9))


    // ==============================================================
    // Step 3: Strategy Generation
    // Generates all possible strategies.
    // Each strategy is a selection of zero or some cache operator(s)
    // ==============================================================
    logInfo("========================================================")
    logInfo("Step 3: Strategy Generation")
    new StrategyGenerator(outputPlans, selectedCEs)
    // We return this, the consumer can pull a strategy and judge its quality (step 4)
  }

  //=======================================================================================
  // Private utility functions
  //=======================================================================================

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
  private def transformToSameDatasource(replaceByRelation: LogicalRelation, tobeReplacedRelation: LogicalRelation, oldPlan: LogicalPlan): LogicalPlan = {
    val standardOutputs = replaceByRelation.output.toList
    val currentOutputs = tobeReplacedRelation.output.toList

    def getCorrespondingAR(a: AttributeReference): AttributeReference = {
      for (item <- standardOutputs) {
        if (item.name == a.name && currentOutputs.contains(a))
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
    // TODO: A bug here, need to track the ID, the getCorrespondingAR function only matches by name
    outPlan = outPlan.transformUp({
      case p => p.transformExpressions({
        case a: AttributeReference => getCorrespondingAR(a)
      })
    })

    outPlan
  }

  /** SEs are grouped by their table signature
    *
    * @param trees
    * @return HashMap<SESignature, ArrayBuffer<(SE, ConsumerQueryIndex)>>
    */
  private def identifySEs(trees: Array[LogicalPlan])
  : mutable.HashMap[BigInteger, mutable.ArrayBuffer[(LogicalPlan, Int)]] = {
    val nPlans = trees.length

    logInfo("Input of %d plans".format(nPlans))
    trees.foreach(p => logInfo("\n" + p.toString()))


    // - `Key`: fingerprint/ signature
    // - `Value`: a set of (logical operator, consumer index)
    // At least 2 logical plans having the same signature are considered as SEs
    // We will produce one group of SEs for each `key` that has the 2 or more `values`
    val fingerPrintMap = new mutable.HashMap[BigInteger, mutable.ArrayBuffer[(LogicalPlan, Int)]]
    def newEmptyList() = new mutable.ArrayBuffer[(LogicalPlan, Int)]()

    // Build a hash tree for each tree
    // key: operator, value: (fingerprint, operator's height)
    // We haven
    val hashTrees = new Array[mutable.HashMap[LogicalPlan, BigInteger]](nPlans)
    trees.indices.foreach { iPlan =>
      hashTrees(iPlan) = buildHashTree(trees(iPlan))
    }

    // Build fingerPrintMap
    trees.indices.foreach(i => {
      var isAllowedToMatchAfter = true // a flag variable
      val visitor = new DFSVisitor(trees(i))
      while (visitor.hasNext) {
        val iPlan = visitor.getNext
        val iPlanFingerprint = hashTrees(i).get(iPlan).get

        val isFoundSE = fingerPrintMap.contains(iPlanFingerprint)

        if (isAllowedToMatchAfter)
          fingerPrintMap.getOrElseUpdate(iPlanFingerprint, newEmptyList()).append(Tuple2(iPlan, i))

        if (isFoundSE && !containsCacheUnfriendlyOperator(iPlan)) {
          isAllowedToMatchAfter = true
        }
        else {
          visitor.goDeeper() // keep looking (doesn't match, or containing cache-unfriendly operator)
          if (isFoundSE && containsCacheUnfriendlyOperator(iPlan)) {
            if (isUnfriendlyOperator(iPlan)) {
              isAllowedToMatchAfter = true // re-enable
            }
            else {
              isAllowedToMatchAfter = false
            }
          }
        }
      }
    })

    // Re-scan one more time. Why? because of the case SE in SE.
    // TODO: explain more
    trees.indices.foreach(i => {
      val visitor = new DFSVisitor(trees(i))

      while (visitor.hasNext) {
        val iPlan = visitor.getNext
        val iPlanFingerprint = hashTrees(i).get(iPlan).get

        val found = (fingerPrintMap.contains(iPlanFingerprint)
          && fingerPrintMap.get(iPlanFingerprint).get.size >= 2)
        visitor.goDeeper()
        if (found) {
          if (!fingerPrintMap.get(iPlanFingerprint).get.contains(Tuple2(iPlan, i))) {
            fingerPrintMap.getOrElseUpdate(iPlanFingerprint, newEmptyList).append(Tuple2(iPlan, i))
          }
        }
      }
    })

    // Keep only those keys that have the length(value) >= 2 (common subtrees)
    val groupedSEs = fingerPrintMap.filter(KVPair => KVPair._2.size >= 2)
    logInfo("Found %d group(s) of SEs".format(groupedSEs.size))
    groupedSEs.foreach(element => {
      logInfo("fingerprint: %d, %d consumers".format(element._1, element._2.length))
      element._2.foreach(e => logInfo("\n" + e._1.toString()))
    })

    groupedSEs
  }

  /**
    * Computes the hash values of a given tree (LogicalPlan)
    * The hash value is computed recursively as the style of Hash Tree (Merkle Tree).
    * * ref: https://en.wikipedia.org/wiki/Merkle_tree
    * The fingerprint (hash value) of an operator is the hash of the "labels" of its children nodes.
    * This is a fast and secure way to search for Similar subExpressions among many trees
    *
    * Note that this function can also be used to compute the finger print of an operator. In such case, hashTree = null
    *
    * @param tree     : input tree
    * @param hashTree : used to store the hash tree
    * @return (fingerprint of the given operator)
    */
  def computeTreeHash(tree: LogicalPlan, hashTree: mutable.HashMap[LogicalPlan, BigInteger] = null): BigInteger = {
    def updateHashTree(treeNode: LogicalPlan, fingerprint: BigInteger): Unit = {
      if (hashTree != null)
        hashTree.put(treeNode, fingerprint)
    }

    val operatorName = tree.getClass.toString
    var fingerprint: BigInteger = null

    tree match {

      // ================================================================
      // Binary Node case: `logicalPlan` has 2 children
      // ================================================================
      case b: BinaryNode =>
        var leftChildHash = computeTreeHash(b.left, hashTree)
        var rightChildHash = computeTreeHash(b.right, hashTree)
        // Unifying the order of left & right child before computing the hash of the parent operator
        // Do the sorting such that the leftChildHash should always lower or equals the rightChildHash
        // Example: We want 2 expressions: (A Join B) and (B Join A) to have the same fingerprint
        if (leftChildHash.compareTo(rightChildHash) > 0) {
          // Do the swap
          val tmpSwap = leftChildHash
          leftChildHash = rightChildHash
          rightChildHash = tmpSwap
        }

        b match {
          case b: Join =>
            // 2 Joins operators can be transformed to share the computation
            // We Consider only same join type and join condition
            fingerprint = Util.hash(operatorName + b.joinType + b.condition + leftChildHash + rightChildHash)

          // override everything, only consider identical expressions
          case b: Intersect =>
            fingerprint = Util.hash(operatorName + b.hashCode().toString)
          case b: Except =>
            fingerprint = Util.hash(operatorName + b.hashCode().toString)
          case _ => throw new NotImplementedError("not supported binary operator: " + operatorName)
        }

      case u: Union =>
        u.children.foreach(c => computeTreeHash(c, hashTree))
        // TODO: In some case, union operators can share some computation
        fingerprint = Util.hash(operatorName + u.hashCode().toString)

      // ================================================================
      // Unary Node case: `logicalPlan` has 1 child
      // ================================================================
      case u: UnaryNode =>
        val childHash = computeTreeHash(u.child, hashTree)
        u match {
          case u@(_: Filter | _: Project) => fingerprint = Util.hash(operatorName + childHash)

          // TODO: can we share some computation in the following operators?
          case u@(_: GlobalLimit | _: LocalLimit | _: Sort | _: Aggregate) => // Only consider identical expression
            fingerprint = Util.hash(operatorName + u.hashCode().toString)

          case _ => // Only consider identical expression
            fingerprint = Util.hash(operatorName + u.hashCode().toString)
        }

      // ================================================================
      // Unary Node case: `logicalPlan` doesn't have any child
      // ================================================================
      case l: LeafNode => l match {
        case leaf: LogicalRelation =>
          val inputPath = Util.extractInputPath(leaf.relation)
          fingerprint = Util.hash(operatorName + inputPath)
        case _ => throw new NotImplementedError("Not supported leaf: " + operatorName)
      }

      case _ => throw new IllegalArgumentException("Not supported logical operator: " + operatorName)
    }

    updateHashTree(tree, fingerprint)
    fingerprint
  }

  /**
    * Computes fingerprints for all sub-trees of rootPlan
    *
    * @param rootPlan
    * @return hash map <subtree, fingerprint>
    */
  private def buildHashTree(rootPlan: LogicalPlan): mutable.HashMap[LogicalPlan, BigInteger] = {
    val res = new mutable.HashMap[LogicalPlan, BigInteger]()
    computeTreeHash(rootPlan, res)
    res
  }

  /** Items need to be put in their class for the MCKP. At most one item from a class can be chosen
    * CEContainers that are dependent will be classified into the same class
    *
    * @param CEContainers
    * @return
    */
  private def classifyCEs(CEContainers: ArrayBuffer[CEContainer]): Array[ItemClass] = {
    val res = new ArrayBuffer[ItemClass]()
    val ces = CEContainers.sortBy(x => Util.getHeight(x.SEs(0)._1))
    val filteredCes = new ArrayBuffer[CEContainer]()

    for (i <- 0 until ces.length) {
      var isDominated = false
      for (j <- i + 1 until ces.length) {
        // checking if ces(i) is descendant of ces(j)
        var isDescendant = false
        for (m <- 0 until ces(i).SEs.length)
          for (n <- 0 until ces(j).SEs.length)
            if (ces(i).SEs(m)._2 == ces(j).SEs(n)._2 && Util.containsDescendant(ces(j).SEs(n)._1, ces(i).SEs(m)._1))
              isDescendant = true

        if (isDescendant) {
          if (ces(i).weight >= ces(j).weight && ces(i).profit <= ces(j).profit) {
            isDominated = true
            //println("is dominated!")
          }
        }
      }

      if (!isDominated) {
        filteredCes.append(ces(i))
      }
      else {
        //println("removed " + ces(i))
      }
    }

    val cesWithAncestors = new mutable.HashMap[CEContainer, mutable.Set[CEContainer]]()
    for (i <- 0 until filteredCes.length) {
      val ancestors = mutable.Set[CEContainer]()
      for (j <- i + 1 until filteredCes.length) {
          var isDescendant = false
          for (m <- 0 until filteredCes(i).SEs.length)
            for (n <- 0 until filteredCes(j).SEs.length)
              if (filteredCes(i).SEs(m)._2 == filteredCes(j).SEs(n)._2 && Util.containsDescendant(filteredCes(j).SEs(n)._1, filteredCes(i).SEs(m)._1))
                isDescendant = true

          if (isDescendant) {
            ancestors += filteredCes(j)
          }

      }
      cesWithAncestors.put(filteredCes(i), ancestors)
    }

    val sortedCes = cesWithAncestors.to[ArrayBuffer].sortBy(x => -x._2.size)

    while (!sortedCes.isEmpty) {
      val isolatedNodes = new ArrayBuffer[(CEContainer, mutable.Set[CEContainer])]()
      val firstCE = sortedCes(0)
      sortedCes.remove(0)
      isolatedNodes.append(firstCE)

      var i = 0
      while (i < sortedCes.length) {
        if (firstCE._2.contains(sortedCes(i)._1) || !firstCE._2.intersect(sortedCes(i)._2).isEmpty) {
          isolatedNodes.append(sortedCes(i))
          sortedCes.remove(i)
        }
        else
          i += 1
      }

      println("isolated: " + isolatedNodes.length)

      // now generate all possible options for the isolated list
      var options = mutable.Set[Set[CEContainer]]()

      for (i <- 0 until isolatedNodes.length) {
        val nodes = ArrayBuffer[CEContainer]()
        nodes.append(isolatedNodes(i)._1)

        for (j <- i+1 until isolatedNodes.length) {
            if(!isolatedNodes(i)._2.contains(isolatedNodes(j)._1)){
              nodes.append(isolatedNodes(j)._1)
            }
        }

        val subOptions = nodes.toSet.subsets.filter(set => {
          val items = set.toList
          if(items.length == 1){
            true
          }
          else if(items.length > 1){
            var keep = true
            for(iItem <- 0 until items.size)
              for(jItem <- 0 until items.size){
                if (iItem != jItem){
                  if(cesWithAncestors(items(iItem)).contains(items(jItem)) ||
                  cesWithAncestors(items(jItem)).contains(items(iItem))){
                    keep = false
                    //println("keep = false")
                  }
                }
              }
            keep
          }
          else
            false
        }).toSet
        options ++= subOptions
      }
      val itemClass = new ItemClass()
      res.append(itemClass)
      options.foreach(option => {
        val list = option.toList
        if(option.size == 1){
          val item = new ItemImpl(list.head.profit, list.head.weight, ArrayBuffer(list.head))
          itemClass.addItem(item)
        }else{
          var profit:Double = 0
          var weight:Double = 0
          val tag = new ArrayBuffer[CEContainer]()
          for(i<- 0 until list.size){
            profit += list(i).profit
            weight += list(i).weight
            tag.append(list(i))
          }
          itemClass.addItem(new ItemImpl(profit, weight, tag))
        }
      })

      println("class i with %d isolated nodes has %d options".format(isolatedNodes.size, options.size))
    }

    res.toArray
  }

  /**
    * checks whether a tree contains a cache-unfriendly operator or not
    *
    * @param plan tree
    * @return
    */
  def containsCacheUnfriendlyOperator(plan: LogicalPlan): Boolean = {
    plan.foreach(f => if (isUnfriendlyOperator(f)) return true)
    false
  }

  /**
    * checks if this tree operator is a cache-unfriendly operator (join/ union)
    *
    * @param operator
    * @return
    */
  private def isUnfriendlyOperator(operator: LogicalPlan): Boolean = {
    operator match {
      case p@(_: Join | _: Union) => true
      case _ => false
    }
  }

  //=======================================================================================
  // END - Private utility functions
  //=======================================================================================


}
