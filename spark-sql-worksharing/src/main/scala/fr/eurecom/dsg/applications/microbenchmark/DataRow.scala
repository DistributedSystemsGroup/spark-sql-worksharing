package fr.eurecom.dsg.applications.microbenchmark

import org.apache.spark.sql.types._
import scala.collection.mutable.ArrayBuffer

/** Schema for the base table in the micro benchmark
  * n1-n10: int
  * d1-d10: double
  * s1-s10: string
  */
class DataRow(val n1: Int, val n2: Int, val n3: Int, val n4: Int, val n5: Int, val n6: Int, val n7: Int, val n8: Int, val n9: Int, val n10: Int,
              val d1: Double, val d2: Double, val d3: Double, val d4: Double, val d5: Double, val d6: Double, val d7: Double, val d8: Double, val d9: Double, val d10: Double,
              val s1: String, val s2: String, val s3: String, val s4: String, val s5: String, val s6: String, val s7: String, val s8: String, val s9: String, val s10: String) extends Product {

  def this(fieldValues: Array[String]) {
    this(fieldValues(0).toInt, fieldValues(1).toInt, fieldValues(2).toInt, fieldValues(3).toInt, fieldValues(4).toInt, fieldValues(5).toInt, fieldValues(6).toInt, fieldValues(7).toInt, fieldValues(8).toInt, fieldValues(9).toInt,
      fieldValues(10).toDouble, fieldValues(11).toDouble, fieldValues(12).toDouble, fieldValues(13).toDouble, fieldValues(14).toDouble, fieldValues(15).toDouble, fieldValues(16).toDouble, fieldValues(17).toDouble, fieldValues(18).toDouble, fieldValues(19).toDouble,
      fieldValues(20), fieldValues(21), fieldValues(22), fieldValues(23), fieldValues(24), fieldValues(25), fieldValues(26), fieldValues(27), fieldValues(28), fieldValues(29))
  }

  override def productElement(n: Int): Any = n match {
    case 0 => n1
    case 1 => n2
    case 2 => n3
    case 3 => n4
    case 4 => n5
    case 5 => n6
    case 6 => n7
    case 7 => n8
    case 8 => n9
    case 9 => n10

    case 10 => d1
    case 11 => d2
    case 12 => d3
    case 13 => d4
    case 14 => d5
    case 15 => d6
    case 16 => d7
    case 17 => d8
    case 18 => d9
    case 19 => d10

    case 20 => s1
    case 21 => s2
    case 22 => s3
    case 23 => s4
    case 24 => s5
    case 25 => s6
    case 26 => s7
    case 27 => s8
    case 28 => s9
    case 29 => s10

    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productArity: Int = 30

  override def canEqual(that: Any): Boolean = that.isInstanceOf[DataRow]
}

object DataRow {
  def getSchema(): StructType = {
    val fields = new ArrayBuffer[StructField]()
    for (i <- 1 until 11) {
      fields.append(StructField("n" + i, IntegerType, nullable = false))
    }
    for (i <- 1 until 11) {
      fields.append(StructField("d" + i, DoubleType, nullable = false))
    }
    for (i <- 1 until 11) {
      fields.append(StructField("s" + i, StringType, nullable = false))
    }

    StructType(fields.toArray)
  }
}