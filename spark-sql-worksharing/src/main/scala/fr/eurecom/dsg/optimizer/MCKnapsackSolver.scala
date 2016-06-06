package fr.eurecom.dsg.optimizer

import scala.collection.mutable.ListBuffer

trait IKnapsackItem {
  def profit:Double
  def weight:Long
}




trait MCKnapsackSolver{
  def optimize(classesOfItems: Array[Array[IKnapsackItem]], capacity:Long):Array[IKnapsackItem]
}

/**
  *
  */
object SimpleMCKPSolver extends MCKnapsackSolver{
  /**
    * KnapsackGreedy
 *
    * @param classesOfItems
    * @param capacity
    * @return List of chosen items
    */
  override def optimize(classesOfItems: Array[Array[IKnapsackItem]], capacity:Long):Array[IKnapsackItem]={
    val chosenItems = new ListBuffer[IKnapsackItem]()
    val bestItemsInEachClass = new ListBuffer[IKnapsackItem]()
    for(c <- classesOfItems.indices){
      bestItemsInEachClass.append(classesOfItems(c).sortBy(item => -item.profit/item.weight).head)
    }
    val sortedItems = bestItemsInEachClass.sortBy(item => -item.profit/item.weight)
    val nItems = sortedItems.length

    var i = 0
    var totalWeight:Long = 0

    while(i < nItems && (totalWeight + sortedItems(i).weight) <= capacity){
      chosenItems +=(sortedItems(i))
      totalWeight += sortedItems(i).weight
      i+=1
    }
    chosenItems.toArray
  }
}
