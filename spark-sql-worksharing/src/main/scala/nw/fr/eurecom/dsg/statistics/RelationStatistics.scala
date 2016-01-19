package nw.fr.eurecom.dsg.statistics

import com.fasterxml.jackson.annotation.{JsonCreator}
import nw.fr.eurecom.dsg.util.{SparkSQLServerLogging, Constants}
import org.apache.spark.sql.catalyst.expressions.Expression
import scala.collection.mutable.HashMap

/**
  * Holds statistics information of a table
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
}
