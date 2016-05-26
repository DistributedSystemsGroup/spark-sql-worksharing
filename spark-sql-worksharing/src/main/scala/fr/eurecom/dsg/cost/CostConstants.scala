package fr.eurecom.dsg.cost

object CostConstants {
  val COST_DISK_READ:Double = 1
  val COST_RAM_READ:Double = 0.01f
  val COST_NETWORK:Double = 2
  val COST_CPU:Double = 0.001
  val COST_MATERIALIZE_TO_RAM = 1
  val COST_HASHING = 1

  val UNKNOWN_COST:Double = -1
  val UNKNOWN:Long = -1
  val CACHE_CAPACITY = 2

  val DEFAULT_SELECTIVITY = 0.33f
}
