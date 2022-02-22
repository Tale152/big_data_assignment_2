package kMeans.versions

import eCP.Java.SiftDescriptorContainer

object BaseKMeans {
  case class BaseKMeansIterationTermination() extends KMeansRandomCentroids {

    private val iterationNumber = 10

    override protected final def endCondition(counter: Int,
                                              previousCentroids: Array[SiftDescriptorContainer],
                                              currentCentroids: Array[SiftDescriptorContainer]): Boolean = {
      counter == iterationNumber
    }
  }
}
