import kMeans.KMeans
import kMeans.versions.EndConditions.{endByMaxReached, endBySimilarity}
import kMeans.versions.InitCentroidSelectors.useFirstN
import kMeans.versions.MapReduces.baseMapReduce
import org.apache.spark.SparkContext
import utils.{Const, ContextFactory}
import utils.LogEnabler.logSelectedOption
import utils.ArgsProvider.{centroidSelector, centroidsNumber, dataPath, endCondition, logFlag, mapReduce, setArgs, sparkMaster}

object Main {

    def main(args: Array[String]): Unit = {
      setArgs(args)
      logSelectedOption(logFlag)
      val sc = ContextFactory.create(Const.appName, sparkMaster, Const.jarPath)
      kMeans(sc)
      sc.stop()
    }

  private def kMeans(sc: SparkContext): Unit = {
    printConfiguration()
    val kMeansExecutor = KMeans(sc, centroidsNumber, dataPath)
    centroidSelector match {
      case "FIRST" => kMeansExecutor.setInitCentroidSelector(useFirstN)
      case _ => throw new IllegalArgumentException("The specified centroid selector does not exist")
    }
    mapReduce match {
      case "DEFAULT" => kMeansExecutor.setMapReduce(baseMapReduce)
      case _ => throw new IllegalArgumentException("The specified map-reduce does not exist")
    }
    endCondition match {
      case "MAX" => kMeansExecutor.setEndCondition(endByMaxReached)
      case "SIMILARITY" => kMeansExecutor.setEndCondition(endBySimilarity)
      case _ => throw new IllegalArgumentException("The specified end condition does not exist")
    }
    kMeansExecutor.compute()
  }

  private def printConfiguration(): Unit = {
    println("==========================")
    println("Running K-Means with:\n")
    println("Centroid selector: " + centroidSelector)
    println("Centroid number: " + centroidsNumber)
    println("Map-Reduce algorithm: " + mapReduce)
    println("End condition: " + endCondition)
    println("==========================\n")
  }
}