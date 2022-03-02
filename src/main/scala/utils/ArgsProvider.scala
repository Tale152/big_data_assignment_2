package utils

import java.io.File

/**
 * List of all flags that can be passed as argument.
 */
private object Flags{
  val SPARK_LOG: String = "-sl"
  val MASTER: String = "-m"
  val DATA: String = "-d"
  val MAP_REDUCE: String = "-mr"
  val END_CONDITION: String = "-ec"
  val CENTROIDS_SELECTOR: String = "-cs"
  val CENTROIDS_NUMBER: String = "-cn"
}

/**
 * List of regex used to associate strings to a particular Flag.
 */
private object Regex{
  val MASTER: String = Flags.MASTER + "=(.*)"
  val DATA: String = Flags.DATA + "=(.*).seq"
  val MAP_REDUCE: String = Flags.MAP_REDUCE + "=(.*)"
  val END_CONDITION: String = Flags.END_CONDITION + "=(.*)"
  val CENTROIDS_SELECTOR: String = Flags.CENTROIDS_SELECTOR + "=(.*)"
  val CENTROIDS_NUMBER: String = Flags.CENTROIDS_NUMBER + "=(.*)"
}

/**
 * Object that follows Singleton pattern to access arguments provided to main in a safe way.
 */
object ArgsProvider{

  private var args: Array[String] = Array[String]()

  /**
   * Needs to be called at least once before using the other methods in order to setup the args source.
   * @param args provided to the main
   */
  def setArgs(args: Array[String]): Unit = {
    this.args = args
  }

  /**
   * @return a Boolean that indicates whether or not activate Spark logs.
   */
  def logFlag: Boolean = {
    val occurrences = countArgs(Flags.SPARK_LOG)
    if (occurrences > 1){
      throw new IllegalArgumentException("Too many " + Flags.SPARK_LOG + "flags provided")
    }
    occurrences == 1
  }

  /**
   * @return the address of the Spark master
   */
  def sparkMaster: String = {
    val occurrences = countArgs(Regex.MASTER)
    throwIf(occurrences == 0, "No spark master provided")
    throwIf(occurrences > 1, "Too many spark masters provided")
    getArg(Regex.MASTER, Flags.MASTER)
  }

  /**
   * @return the path where the .seq file is stored
   */
  def dataPath: String = {
    val occurrences = countArgs(Regex.DATA)
    throwIf(occurrences == 0, "No .seq file provided")
    throwIf(occurrences > 1, "More than one .seq file provided")
    val path = getArg(Regex.DATA, Flags.DATA)
    throwIf(!fileExists(path), path + " file does not exist")
    path
  }

  /**
   * @return the number ok centroids to use in the computation
   */
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

  /**
   * @return a string that specifies what centroid selector to use in the computation
   */
  def centroidSelector: String = selectionWithDefault(Regex.CENTROIDS_SELECTOR, Flags.CENTROIDS_SELECTOR, "FIRST_N")

  /**
   * @return a string that specifies what end condition to use in the computation
   */
  def endCondition: String = selectionWithDefault(Regex.END_CONDITION, Flags.END_CONDITION, "MAX")

  /**
   * @return a string that specifies what map-reduce algorithm to use in the computation
   */
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
