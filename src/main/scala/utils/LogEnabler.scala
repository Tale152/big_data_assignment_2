package utils

import org.apache.log4j.{Level, Logger}

/**
 * Utility object that contains a method to turn on/off the Spark's Logger.
 */
object LogEnabler {

  /**
   * Turns on/off the Spark's Logger.
   * @param flag that turns on/off the Spark's Logger
   */
  def logSelectedOption(flag: Boolean): Unit = if(!flag){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
  }
}
