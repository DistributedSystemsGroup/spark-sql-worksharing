package fr.eurecom.dsg.cost

object CostConstants {
  val COST_DISK_READ:Double = 1

  val COST_NETWORK:Double = 2
  val COST_SIMPLE_OP:Double = 0.001

  val COST_RAM_READ:Double = 0.01f
  val COST_RAM_WRITE = 1

  val DEFAULT_SELECTIVITY:Double = 0.33f


  val MIN_SAVING:Double = 20000f
//  val MAX_CACHE_SIZE:Long = 1000000000 // 1 GB
  val MAX_CACHE_SIZE:Long = 400000000 // 1 GB
}
