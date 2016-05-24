package nw.fr.eurecom.dsg.optimizer
import scala.collection.mutable.ListBuffer

trait KnapsackItem{
  def profit:Double
  def weight:Long
}

trait KnapsackSolver{
  def optimize(items: Array[KnapsackItem], knapsackWeight:Long):Array[KnapsackItem]
}

/**
  *
  */
object KnapsackGreedy extends KnapsackSolver{
  /**
    * KnapsackGreedy
    * @param items
    * @param knapsackWeight
    * @return List of chosen items
    */
  override def optimize(items: Array[KnapsackItem], knapsackWeight:Long):Array[KnapsackItem]={
    val chosenItems = new ListBuffer[KnapsackItem]()
    val nItems = items.length
    val sortedItems = items.sortBy(k => k.profit * -1 / k.weight)
    var i = 0
    var currentWeight:Long = 0
    while(i < nItems && currentWeight < knapsackWeight){
      chosenItems.+=(sortedItems(i))
      currentWeight += sortedItems(i).weight
      i+=1
    }
    chosenItems.toArray
  }
}
