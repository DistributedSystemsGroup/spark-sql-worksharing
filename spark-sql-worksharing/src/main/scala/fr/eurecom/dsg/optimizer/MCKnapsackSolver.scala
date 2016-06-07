package fr.eurecom.dsg.optimizer

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class KnapsackItem {
  var profit:Double = 0
  var weight:Double = 0
  var content:ArrayBuffer[Object] = new ArrayBuffer[Object]()
}


class KnapsackClass(){
  val items = new ArrayBuffer[KnapsackItem]()
  def nItems = items.length
  def addItem(item: KnapsackItem): Unit ={
    items.append(item)
  }
}

trait MCKnapsackSolver{
  def optimize(classesOfItems: Array[KnapsackClass], capacity:Long):Array[KnapsackItem]
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
  override def optimize(classesOfItems: Array[KnapsackClass], capacity:Long):Array[KnapsackItem]={
    val chosenItems = new ListBuffer[KnapsackItem]()
    val bestItemsInEachClass = new ListBuffer[KnapsackItem]()
    for(c <- classesOfItems.indices){
      bestItemsInEachClass.append(classesOfItems(c).items.sortBy(item => -item.profit/item.weight).head)
    }
    val sortedItems = bestItemsInEachClass.sortBy(item => -item.profit/item.weight)
    val nItems = sortedItems.length

    var i = 0
    var totalWeight:Double = 0

    while(i < nItems && (totalWeight + sortedItems(i).weight) <= capacity){
      chosenItems += sortedItems(i)
      totalWeight += sortedItems(i).weight
      i+=1
    }
    chosenItems.toArray
  }
}
