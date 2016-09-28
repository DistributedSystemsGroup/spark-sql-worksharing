package fr.eurecom.dsg.cost

object CostConstants {
  val COST_DISK_READ:Double = 0.1f // care disk cache of OS
  val COST_NETWORK:Double = 10f // should be expensive
  val COST_SIMPLE_OP:Double = 1e-6f // should be small // filter, project

  val COST_RAM_READ:Double = 1e-3f // RAM READ = 100x DISK READ
  val COST_RAM_WRITE = 0.1f

  val DEFAULT_SELECTIVITY:Double = 0.33f

  val MIN_SAVING:Double = 200000f
  val MAX_SE_OUTPUT_SIZE_GB = 5
  val MAX_CACHE_SIZE_GB:Int = 30 // 20 GB
}
