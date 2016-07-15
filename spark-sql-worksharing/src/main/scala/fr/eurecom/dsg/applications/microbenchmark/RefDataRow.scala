package fr.eurecom.dsg.applications.microbenchmark

import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by ntkhoa on 30/06/16.
  */
class RefDataRow(val ref_n1:Int, val ref_n2:Int, val ref_n3:Int, val ref_n4:Int, val ref_n5:Int, val ref_n6:Int, val ref_n7:Int, val ref_n8:Int, val ref_n9:Int, val ref_n10:Int,
              val ref_d1:Double, val ref_d2:Double, val ref_d3:Double, val ref_d4:Double, val ref_d5:Double, val ref_d6:Double, val ref_d7:Double, val ref_d8:Double, val ref_d9:Double, val ref_d10:Double,
              val ref_s1:String, val ref_s2:String, val ref_s3:String, val ref_s4:String, val ref_s5:String, val ref_s6:String, val ref_s7:String, val ref_s8:String, val ref_s9:String, val ref_s10:String) extends Product{

  def this(text:Array[String]){
    this(text(0).toInt, text(1).toInt, text(2).toInt, text(3).toInt, text(4).toInt, text(5).toInt, text(6).toInt, text(7).toInt, text(8).toInt, text(9).toInt,
      text(10).toDouble, text(11).toDouble, text(12).toDouble, text(13).toDouble, text(14).toDouble, text(15).toDouble, text(16).toDouble, text(17).toDouble, text(18).toDouble, text(19).toDouble,
      text(20), text(21), text(22), text(23), text(24), text(25), text(26), text(27), text(28), text(29))
  }

  override def productElement(n: Int): Any = n match{
    case 0=> ref_n1
    case 1=> ref_n2
    case 2=> ref_n3
    case 3=> ref_n4
    case 4=> ref_n5
    case 5=> ref_n6
    case 6=> ref_n7
    case 7=> ref_n8
    case 8=> ref_n9
    case 9=> ref_n10

    case 10 => ref_d1
    case 11 => ref_d2
    case 12 => ref_d3
    case 13 => ref_d4
    case 14 => ref_d5
    case 15 => ref_d6
    case 16 => ref_d7
    case 17 => ref_d8
    case 18 => ref_d9
    case 19 => ref_d10

    case 20 => ref_s1
    case 21 => ref_s2
    case 22 => ref_s3
    case 23 => ref_s4
    case 24 => ref_s5
    case 25 => ref_s6
    case 26 => ref_s7
    case 27 => ref_s8
    case 28 => ref_s9
    case 29 => ref_s10

    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productArity: Int = 30

  override def canEqual(that: Any): Boolean = that.isInstanceOf[RefDataRow]
}

object RefDataRow{
  def getSchema():StructType = {
    val fields = new ArrayBuffer[StructField]()
    for(i <- 1 until 11){
      fields.append(StructField("ref_n" + i, IntegerType, false))
    }
    for(i <- 1 until 11){
      fields.append(StructField("ref_d" + i, DoubleType, false))
    }
    for(i <- 1 until 11){
      fields.append(StructField("ref_s" + i, StringType, false))
    }

    StructType(fields.toArray)
  }
}