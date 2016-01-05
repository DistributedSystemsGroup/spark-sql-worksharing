package nw.fr.eurecom.dsg.cost

object CostConstants {
  val COST_DISK_READ:Double = 1
  val COST_RAM_READ:Double = 0.001f
  val COST_NETWORK:Double = 2
  val COST_CPU:Double = 1
  val COST_MATERIALIZE_TO_RAM = 1
  val COST_HASHING = 1
  val COST_SORTING_FACTOR = 1

  val UNKNOWN_COST:Double = 0
  val UNKNOWN:Long = 0

  val MAX_CACHE_CAPACITY = 2

}
