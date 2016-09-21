package fr.eurecom.dsg.cost

object CostConstants {
  val COST_DISK_READ:Double = 0.1f // care disk cache of OS
  val COST_NETWORK:Double = 5f // should be expensive
  val COST_SIMPLE_OP:Double = 0.00001f // should be small

  val COST_RAM_READ:Double = 0.001f // RAM READ = 100x DISK READ
  val COST_RAM_WRITE = 0.1f

  val DEFAULT_SELECTIVITY:Double = 0.33f

  val MIN_SAVING:Double = 200000f
  val MAX_CACHE_SIZE_GB:Int = 30 // 20 GB
}
