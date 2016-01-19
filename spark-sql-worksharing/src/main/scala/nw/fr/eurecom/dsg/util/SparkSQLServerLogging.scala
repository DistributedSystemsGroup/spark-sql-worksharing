package nw.fr.eurecom.dsg.util
import org.apache.spark.Logging

trait SparkSQLServerLogging extends Logging{
  val NAME = "SparkSQLServer"

  private def mkMsgWithPrefix(msg: =>String)={
    "[%s]\n%s\n[/%s]".format(NAME, msg, NAME)
  }

  override protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(mkMsgWithPrefix(msg))
  }

  override protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(mkMsgWithPrefix(msg))
  }

  override protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(mkMsgWithPrefix(msg))
  }

  override protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(mkMsgWithPrefix(msg))
  }

  override protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(mkMsgWithPrefix(msg))
  }
}
