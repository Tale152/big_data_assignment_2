package kMeans

import kMeans.versions.EndConditions.{endByMaxReached, endBySimilarity}
import kMeans.versions.InitCentroidSelectors.{useEvenlySpaced, useFirstN}
import kMeans.versions.MapReduces.{baseMapReduce, earlyHaltingMapReduce, earlyHaltingUnravelingMapReduce}
import org.apache.spark.SparkContext
import utils.ArgsProvider

object Computation {

  def kMeans(sc: SparkContext): Unit = {
    var kMeansBuilder = KMeansBuilder()
      .sparkContext(sc)
      .centroidsNumber(ArgsProvider.centroidsNumber)
      .dataPath(ArgsProvider.dataPath)

    kMeansBuilder = setEndCondition(setMapReduce(setInitCentroids(kMeansBuilder)))
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

  private def setInitCentroids(kMeansBuilder: KMeansBuilder): KMeansBuilder = ArgsProvider.centroidSelector match {
    case "FIRST_N" => kMeansBuilder.initCentroidSelector(useFirstN)
    case "EVENLY_SPACED" => kMeansBuilder.initCentroidSelector(useEvenlySpaced)
    case _ => throw new IllegalArgumentException("The specified centroid selector does not exist")
  }

  private def setMapReduce(kMeansBuilder: KMeansBuilder): KMeansBuilder = ArgsProvider.mapReduce match {
    case "DEFAULT" => kMeansBuilder.mapReduce(baseMapReduce)
    case "EARLY_HALTING" => kMeansBuilder.mapReduce(earlyHaltingMapReduce)
    case "EARLY_HALTING_UNRAVELING" => kMeansBuilder.mapReduce(earlyHaltingUnravelingMapReduce)
    case _ => throw new IllegalArgumentException("The specified map-reduce does not exist")
  }

  private def setEndCondition(kMeansBuilder: KMeansBuilder): KMeansBuilder = ArgsProvider.endCondition match {
    case "MAX" => kMeansBuilder.endCondition(endByMaxReached)
    case "SIMILARITY" => kMeansBuilder.endCondition(endBySimilarity)
    case _ => throw new IllegalArgumentException("The specified end condition does not exist")
  }

}
