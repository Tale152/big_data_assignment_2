package utils

import kMeans.versions.KMeansVersions

import java.io.File

private object Flags{
  val SPARK_LOG: String = "-sl"
  val MASTER: String = "-m"
  val DATA: String = "-d"
  val VERSION: String = "-v"
  val CENTROIDS: String = "-c"
}

private object Regex{
  val MASTER: String = Flags.MASTER + "=spark://(.*)"
  val DATA: String = Flags.DATA + "=(.*).seq"
  val VERSION: String = Flags.VERSION + "=(.*)"
  val CENTROIDS: String = Flags.CENTROIDS + "=(.*)"
}

object ArgsProvider{

  private var args: Array[String] = Array[String]()

  def setArgs(args: Array[String]): Unit = {
    this.args = args
  }

  def logFlag: Boolean = {
    val occurrences = countArgs(Flags.SPARK_LOG)
    if (occurrences > 1){
      throw new IllegalArgumentException("Too many " + Flags.SPARK_LOG + "flags provided")
    }
    occurrences == 1
  }

  def sparkMaster: String = {
    val occurrences = countArgs(Regex.MASTER)
    throwIf(occurrences == 0, "No spark master provided")
    throwIf(occurrences > 1, "Too many spark masters provided")
    getArg(Regex.MASTER, Flags.MASTER)
  }

  def dataPath: String = {
    val occurrences = countArgs(Regex.DATA)
    throwIf(occurrences == 0, "No .seq file provided")
    throwIf(occurrences > 1, "More than one .seq file provided")
    val path = getArg(Regex.DATA, Flags.DATA)
    throwIf(!fileExists(path), path + " file does not exist")
    path
  }

  def version: String = {
    val occurrences = countArgs(Regex.VERSION)
    if(occurrences == 0){
      KMeansVersions.DEFAULT
    } else {
      throwIf(occurrences > 1, "More than one " + Flags.VERSION + " flag provided")
      getArg(Regex.VERSION, Flags.VERSION).toUpperCase()
    }
  }

  def nCentroids: Int = {
    val occurrences = countArgs(Regex.CENTROIDS)
    if(occurrences == 0){
      100
    } else {
      throwIf(occurrences > 1, "More than one " + Flags.CENTROIDS + " flag provided")
      val nString = getArg(Regex.CENTROIDS, Flags.CENTROIDS)
      throwIf(nString.matches("(.*)[a-z]+(.*)"), Flags.CENTROIDS + " has to be an int value")
      val res = Integer.parseInt(nString)
      throwIf(res <= 0, Flags.CENTROIDS + " has to be greater than zero")
      res
    }
  }

  private def throwIf(condition: Boolean, msg: String): Unit = if(condition){
    throw new IllegalArgumentException(msg)
  }

  private def countArgs(regex: String): Int = args.count(s => s.matches(regex))

  private def getArg(regex: String, flag: String): String = args.find(s => s.matches(regex)).get.replace(flag + "=", "")

  private def fileExists(path: String):Boolean = new File(path).exists()

}
