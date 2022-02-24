package kMeans

import kMeans.versions.EndConditions.{endByMaxReached, endBySimilarity}
import kMeans.versions.InitCentroidSelectors.{useEvenlySpaced, useFirstN}
import kMeans.versions.MapReduces.{baseMapReduce, earlyHaltingMapReduce}
import org.apache.spark.SparkContext
import utils.ArgsProvider

object Computation {

  def kMeans(sc: SparkContext): Unit = {
    val kMeansExecutor = KMeans(sc, ArgsProvider.centroidsNumber, ArgsProvider.dataPath)
    ArgsProvider.centroidSelector match {
      case "FIRST_N" => kMeansExecutor.setInitCentroidSelector(useFirstN)
      case "EVENLY_SPACED" => kMeansExecutor.setInitCentroidSelector(useEvenlySpaced)
      case _ => throw new IllegalArgumentException("The specified centroid selector does not exist")
    }
    ArgsProvider.mapReduce match {
      case "DEFAULT" => kMeansExecutor.setMapReduce(baseMapReduce)
      case "EARLY_HALTING" => kMeansExecutor.setMapReduce(earlyHaltingMapReduce)
      case _ => throw new IllegalArgumentException("The specified map-reduce does not exist")
    }
    ArgsProvider.endCondition match {
      case "MAX" => kMeansExecutor.setEndCondition(endByMaxReached)
      case "SIMILARITY" => kMeansExecutor.setEndCondition(endBySimilarity)
      case _ => throw new IllegalArgumentException("The specified end condition does not exist")
    }
    kMeansExecutor.compute()
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
