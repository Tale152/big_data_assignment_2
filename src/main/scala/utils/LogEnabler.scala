package utils

import org.apache.log4j.{Level, Logger}

object LogEnabler {

  def logSelectedOption(flag: Boolean): Unit = if(!flag){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
  }
}
