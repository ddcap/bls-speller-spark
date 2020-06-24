package be.ugent.intec.ddecap

import org.apache.log4j._

@SerialVersionUID(269L)
trait Logging extends Serializable {
  @transient lazy private[this] val logger = Logger.getLogger(getClass().getName());

  import org.apache.log4j.Level._

  def debug(message: => String) = logger.debug(message)
  def debug(message: => String, ex:Throwable) = logger.debug(message,ex)
  def debugValue[T](valueName: String, value: => T):T = {
    val result:T = value
    debug(valueName + " == " + result.toString)
    result
  }

  def info(message: => String) = logger.info(message)
  def info(message: => String, ex:Throwable) = logger.info(message,ex)

  def warn(message: => String) =  logger.warn(message)
  def warn(message: => String, ex:Throwable) =  logger.warn(message,ex)

  def error(ex:Throwable) =  logger.error(ex.toString,ex)
  def error(message: => String) =  logger.error(message)
  def error(message: => String, ex:Throwable) =  logger.error(message,ex)

  def fatal(ex:Throwable) = logger.fatal(ex.toString,ex)
  def fatal(message: => String) = logger.fatal(message)
  def fatal(message: => String, ex:Throwable) =  logger.fatal(message,ex)
}
