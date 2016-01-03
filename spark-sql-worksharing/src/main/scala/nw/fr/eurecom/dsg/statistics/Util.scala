package nw.fr.eurecom.dsg.statistics

/**
  * Created by ntkhoa on 03/01/16.
  */
object Util {
  def stringToInt(key:String, mod:Int):Int = {
    val res = key.hashCode() % mod
    if(res < 0)
      res + mod
    else
      res
  }
}
