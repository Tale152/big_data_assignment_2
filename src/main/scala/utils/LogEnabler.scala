package utils

import org.apache.log4j.{Level, Logger}

object LogEnabler {

  def logSelectedOption(args: Array[String]): Unit = {
    //TODO check args
    val logDisabled = true
    if(logDisabled){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }
  }
}
