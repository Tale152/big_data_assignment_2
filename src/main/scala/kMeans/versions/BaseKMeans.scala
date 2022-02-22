package kMeans.versions

import eCP.Java.SiftDescriptorContainer
import kMeans.VectorOperation.similarity

object BaseKMeans {
  private val iterationNumber = 20

  case class BaseKMeansIterationTermination() extends KMeansRandomCentroids {

    override protected final def endCondition(counter: Int,
                                              previousCentroids: Array[SiftDescriptorContainer],
                                              currentCentroids: Array[SiftDescriptorContainer]): Boolean = {
      counter == iterationNumber
    }
  }

  case class BaseKMeansCentroidsTermination() extends KMeansRandomCentroids {
    override protected final def endCondition(counter: Int,
                                              previousCentroids: Array[SiftDescriptorContainer],
                                              currentCentroids: Array[SiftDescriptorContainer]): Boolean = {
      if (counter == 0) {
        counter == iterationNumber
      } else {
        (counter == iterationNumber) || similarity(currentCentroids, previousCentroids, 100)
      }
    }
  }
}
