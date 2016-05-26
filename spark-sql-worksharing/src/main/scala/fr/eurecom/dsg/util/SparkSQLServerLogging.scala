package fr.eurecom.dsg.util
import org.slf4j.LoggerFactory

trait SparkSQLServerLogging{

  val log = LoggerFactory.getLogger(getClass.getName)

  val NAME = "SparkSQLServer"

  private def mkMsgWithPrefix(msg: =>String)={
    "[%s]\n%s\n[/%s]".format(NAME, msg, NAME)
  }

  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(mkMsgWithPrefix(msg))
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(mkMsgWithPrefix(msg))
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(mkMsgWithPrefix(msg))
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(mkMsgWithPrefix(msg))
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(mkMsgWithPrefix(msg))
  }
}
