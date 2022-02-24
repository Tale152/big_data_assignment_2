package kMeans.versions

import kMeans.VectorOperation.similarity
import utils.Point

object EndConditions {

  private val max = 40

  def endByMaxReached(counter: Int,
                      previousCentroids: Array[Point],
                      currentCentroids: Array[Point]): Boolean = counter == max

  def endBySimilarity(counter: Int,
                   previousCentroids: Array[Point],
                   currentCentroids: Array[Point]): Boolean = {
    if (counter > 0) {
      (counter == max) || similarity(currentCentroids, previousCentroids, 50)
    } else {
      false
    }
  }

}
