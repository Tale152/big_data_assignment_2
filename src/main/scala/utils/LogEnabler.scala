package utils

import org.apache.log4j.{Level, Logger}

object LogEnabler {

  def logSelectedOption(args: Array[String]): Unit = {
    if(!args.contains("-log")){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }
  }
}
