package utils

import java.io.File

private object Flags{
  val SPARK_LOG: String = "-sl"
  val MASTER: String = "-m"
  val DATA: String = "-d"
  val MAP_REDUCE: String = "-mr"
  val END_CONDITION: String = "-ec"
  val CENTROIDS_SELECTOR: String = "-cs"
  val CENTROIDS_NUMBER: String = "-cn"
}

private object Regex{
  val MASTER: String = Flags.MASTER + "=spark://(.*)"
  val DATA: String = Flags.DATA + "=(.*).seq"
  val MAP_REDUCE: String = Flags.MAP_REDUCE + "=(.*)"
  val END_CONDITION: String = Flags.END_CONDITION + "=(.*)"
  val CENTROIDS_SELECTOR: String = Flags.CENTROIDS_SELECTOR + "=(.*)"
  val CENTROIDS_NUMBER: String = Flags.CENTROIDS_NUMBER + "=(.*)"
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

  def centroidsNumber: Int = {
    val occurrences = countArgs(Regex.CENTROIDS_NUMBER)
    if(occurrences == 0){
      100
    } else {
      throwIf(occurrences > 1, "More than one " + Flags.CENTROIDS_NUMBER + " flag provided")
      val nString = getArg(Regex.CENTROIDS_NUMBER, Flags.CENTROIDS_NUMBER)
      throwIf(nString.matches("(.*)[a-z]+(.*)"), Flags.CENTROIDS_NUMBER + " has to be an int value")
      val res = Integer.parseInt(nString)
      throwIf(res <= 0, Flags.CENTROIDS_NUMBER + " has to be greater than zero")
      res
    }
  }

  def centroidSelector: String = selectionWithDefault(Regex.CENTROIDS_SELECTOR, Flags.CENTROIDS_SELECTOR, "FIRST_N")

  def endCondition: String = selectionWithDefault(Regex.END_CONDITION, Flags.END_CONDITION, "MAX")

  def mapReduce: String = selectionWithDefault(Regex.MAP_REDUCE, Flags.MAP_REDUCE, "DEFAULT")

  private def selectionWithDefault(regex: String, flag: String, default: String): String = {
    val occurrences = countArgs(regex)
    if(occurrences == 0){
      default
    } else {
      throwIf(occurrences > 1, "More than one " + flag + " flag provided")
      getArg(regex, flag).toUpperCase()
    }
  }

  private def throwIf(condition: Boolean, msg: String): Unit = if(condition){
    throw new IllegalArgumentException(msg)
  }

  private def countArgs(regex: String): Int = args.count(s => s.matches(regex))

  private def getArg(regex: String, flag: String): String = args.find(s => s.matches(regex)).get.replace(flag + "=", "")

  private def fileExists(path: String):Boolean = new File(path).exists()

}
