package nw.fr.eurecom.dsg.statistics

import scala.util.Try

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

def TryToInt(s:String)=Try(s.toInt).toOption
def TryToLong(s:String)=Try(s.toLong).toOption
def TryToDouble(s:String)=Try(s.toDouble).toOption


}
