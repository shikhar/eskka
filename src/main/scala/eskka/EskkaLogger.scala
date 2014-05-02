package eskka

import akka.actor.Actor
import akka.event.DummyClassForStringSources
import akka.event.Logging._

import org.elasticsearch.common.logging.{ ESLogger, ESLoggerFactory }

class EskkaLogger extends Actor {

  private[this] val loggers = collection.mutable.Map[Class[_], ESLogger]()

  private def logger(cls: Class[_]) =
    loggers.getOrElseUpdate(cls, ESLoggerFactory.getLogger(if (cls == classOf[DummyClassForStringSources]) "akka" else cls.getName))

  override def receive = {

    case InitializeLogger(_) =>
      sender() ! LoggerInitialized

    case event @ Error(cause, logSource, logClass, message) =>
      val log = logger(logClass)
      if (log.isErrorEnabled) {
        log.error(s"[$logSource] $message", cause)
      }

    case event @ Warning(logSource, logClass, message) =>
      val log = logger(logClass)
      if (log.isWarnEnabled) {
        log.warn(s"[$logSource] $message")
      }

    case event @ Info(logSource, logClass, message) =>
      val log = logger(logClass)
      if (log.isInfoEnabled) {
        log.info(s"[$logSource] $message")
      }

    case event @ Debug(logSource, logClass, message) =>
      val log = logger(logClass)
      if (log.isDebugEnabled) {
        log.debug(s"[$logSource] $message")
      }

  }

}
