package fr.eurecom.dsg.optimizer

import scala.collection.mutable.ArrayBuffer

trait Item{
  def profit: Double
  def weight: Double
}

class ItemImpl(p: Double, w: Double, _tag:Object = null) extends Item{
  override def profit: Double = p
  override def weight: Double = w
  def tag = _tag
}

object NullItem extends Item{
  override def profit: Double = 0
  override def weight: Double = 0
}

class ItemClass(_items:Array[Item]){
  val items:ArrayBuffer[Item] = new ArrayBuffer[Item]()
  items.append(NullItem)
  _items.foreach(i => items.append(i))

  def this()= this(Array.empty[Item])

  def addItem(i:Item):Unit = {
    items.append(i)
  }
}

object MCKnapsackSolverTmp {

  /**
    *
    * check example in the code above to know how to pass the classes
    * Applying Dynamic Programming technique
    * @param knapsackCapacity
    * @param _classes
    * @return
    */
  def solve(knapsackCapacity:Int, _classes:Array[ItemClass]): Array[Item] ={
    val classes = new ArrayBuffer[ItemClass]()
    classes.append(new ItemClass())
    _classes.foreach(c => classes.append(c))

    val nClasses = classes.length // true class + empty class
    val z = Array.fill[(Int, Int)](nClasses, knapsackCapacity + 1)((Int.MinValue, -1))
    for (i <- 0 until knapsackCapacity + 1){
      z(0)(i) = (0, -1)
    }

    for (i <- 1 until nClasses){
      for (j <- 0 until knapsackCapacity + 1){
        val items = classes(i).items
        for (k <- 0 until items.length){
          val weight_itemK_ClassI = items(k).weight
          if(weight_itemK_ClassI <= j){
            val option1 = z(i)(j)
            val option2 = (z(i-1)(j - weight_itemK_ClassI.toInt)._1 + items(k).profit.toInt, k)

            if (option1._1 <= option2._1)
              z(i)(j) = option2
          }
        }
      }
    }

     //For debugging: print array of computation result
    for(i <- 0 until nClasses){
      for (j <- 0 until knapsackCapacity + 1)
        print(z(i)(j))
      println
    }

    val solution = new ArrayBuffer[Item]()
    var capacityLeft = knapsackCapacity

    for(iClass <- nClasses - 1 until 0 by -1){
      val tuple = z(iClass)(capacityLeft)
      if(tuple._2 != 0){ // dont pick the null item
        val chosenItem = classes(iClass).items(tuple._2)
        solution.append(chosenItem)
        capacityLeft = capacityLeft - chosenItem.weight.toInt
      }
    }

    solution.toArray
  }

  def main(args: Array[String]): Unit = {

    val C = 15
    val sampleClasses = Array[ItemClass](new ItemClass(),
      new ItemClass(Array(new ItemImpl(15, 8), new ItemImpl(11, 4), new ItemImpl(5, 4), new ItemImpl(8, 3))),
      new ItemClass(Array(new ItemImpl(12, 5), new ItemImpl(18, 14), new ItemImpl(20, 11), new ItemImpl(14, 5))),
      new ItemClass(Array(new ItemImpl(8, 4), new ItemImpl(9, 6), new ItemImpl(6, 3), new ItemImpl(10, 5))),
      new ItemClass(Array(new ItemImpl(50, 1), new ItemImpl(7, 5))),
      new ItemClass(Array(new ItemImpl(2, 3), new ItemImpl(3, 2), new ItemImpl(6, 9), new ItemImpl(5, 7)))
    )
    val chosenItems = solve(C, sampleClasses)
    chosenItems.foreach(i => println(i.weight, " ", i.profit))
  }

}
