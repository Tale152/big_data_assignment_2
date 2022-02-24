package kMeans

import kMeans.versions.EndConditions.{endByMaxReached, endBySimilarity}
import kMeans.versions.InitCentroidSelectors.{useEvenlySpaced, useFirstN}
import kMeans.versions.MapReduces.{baseMapReduce, earlyHaltingMapReduce}
import org.apache.spark.SparkContext
import utils.ArgsProvider

object Computation {

  def kMeans(sc: SparkContext): Unit = {
    var kMeansBuilder = KMeansBuilder()
      .sparkContext(sc)
      .centroidsNumber(ArgsProvider.centroidsNumber)
      .dataPath(ArgsProvider.dataPath)

    ArgsProvider.centroidSelector match {
      case "FIRST_N" => kMeansBuilder = kMeansBuilder.initCentroidSelector(useFirstN)
      case "EVENLY_SPACED" => kMeansBuilder = kMeansBuilder.initCentroidSelector(useEvenlySpaced)
      case _ => throw new IllegalArgumentException("The specified centroid selector does not exist")
    }
    ArgsProvider.mapReduce match {
      case "DEFAULT" => kMeansBuilder = kMeansBuilder.mapReduce(baseMapReduce)
      case "EARLY_HALTING" => kMeansBuilder = kMeansBuilder.mapReduce(earlyHaltingMapReduce)
      case _ => throw new IllegalArgumentException("The specified map-reduce does not exist")
    }
    ArgsProvider.endCondition match {
      case "MAX" => kMeansBuilder = kMeansBuilder.endCondition(endByMaxReached)
      case "SIMILARITY" => kMeansBuilder = kMeansBuilder.endCondition(endBySimilarity)
      case _ => throw new IllegalArgumentException("The specified end condition does not exist")
    }

    kMeansBuilder.build().compute()
  }

  def printConfiguration(): Unit = {
    println("==========================")
    println("Running K-Means with:\n")
    println("Centroid selector: " + ArgsProvider.centroidSelector)
    println("Centroid number: " + ArgsProvider.centroidsNumber)
    println("Map-Reduce algorithm: " + ArgsProvider.mapReduce)
    println("End condition: " + ArgsProvider.endCondition)
    println("==========================\n")
  }
}
