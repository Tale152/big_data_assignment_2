package kMeans.versions

import eCP.Java.SiftDescriptorContainer
import kMeans.VectorOperation.similarity

object EndConditions {

  private val max = 40

  def endByMaxReached(counter: Int,
                      previousCentroids: Array[SiftDescriptorContainer],
                      currentCentroids: Array[SiftDescriptorContainer]): Boolean = counter == max

  def endBySimilarity(counter: Int,
                   previousCentroids: Array[SiftDescriptorContainer],
                   currentCentroids: Array[SiftDescriptorContainer]): Boolean = {
    if (counter > 0) {
      (counter == max) || similarity(currentCentroids, previousCentroids, 50)
    } else {
      false
    }
  }

}
