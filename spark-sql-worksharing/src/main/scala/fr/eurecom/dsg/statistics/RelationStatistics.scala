package fr.eurecom.dsg.statistics

import com.fasterxml.jackson.annotation.JsonCreator
import fr.eurecom.dsg.util.Constants
import scala.collection.mutable.HashMap

/** POJO class
  * Holds statistics information of a table
  *
  * @param inputSize total size of the input in bytes
  * @param numRecords total number of records of the table
  * @param averageRecSize average size of records in bytes
  * @param columnStats column statistics for this table
  */
@JsonCreator
class RelationStatistics(val inputSize:Long = Constants.UNKNOWN_VAL,
                         val numRecords:Long = Constants.UNKNOWN_VAL,
                         val averageRecSize:Double = Constants.UNKNOWN_VAL_DOUBLE,
                         val columnStats:HashMap[String, ColumnStatistics]){
  def getColumnStats(columnName:String):ColumnStatistics={
    columnStats.getOrElse(columnName, null)
  }

  override def toString():String = {
    val sb = new StringBuilder()
    sb.append("InputSize=%d, NumRecords=%d, AverageRecSize=%f".format(inputSize, numRecords, averageRecSize))
    columnStats.foreach(kv => sb.append(" \n" + kv._1 + " " + kv._2.toString()))
    sb.toString()
  }
}
