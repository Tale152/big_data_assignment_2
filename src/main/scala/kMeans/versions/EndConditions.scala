package kMeans.versions

import kMeans.VectorOperation.similarity
import utils.Point

/**
 * Object that contains various implementation for the end condition strategy to provide to [[kMeans.KMeansBuilder]] as
 * argument in the [[kMeans.KMeansBuilder.endCondition]] method.
 */
object EndConditions {

  private val max = 40

  /**
   * Checks if the K-Means algorithm is over confronting the counter to the max to reach (40).
   * @param counter number of iterations of the K-Means algorithm
   * @param previousCentroids Array containing instances of the centroids at the previous iteration of the K-Means algorithm
   * @param currentCentroids Array containing instances of the centroids at the current iteration of the K-Means algorithm
   * @return weather the K-Means algorithm has to stop (true) or it needs to do another iteration (false)
   * @see [[kMeans.KMeansBuilder.endCondition]]
   */
  def endByMaxReached(counter: Int, previousCentroids: Array[Point], currentCentroids: Array[Point]): Boolean ={
    counter == max
  }

  /**
   * Checks if the K-Means algorithm is over confronting the centroids current centroids and the one coming from the previous iteration;
   * if they are similar (with a tolerance of 50), the K-Means can end.
   * In case the similarity is never reached, the computation will stop when the counter reaches the max (40).
   * @param counter number of iterations of the K-Means algorithm
   * @param previousCentroids Array containing instances of the centroids at the previous iteration of the K-Means algorithm
   * @param currentCentroids Array containing instances of the centroids at the current iteration of the K-Means algorithm
   * @return weather the K-Means algorithm has to stop (true) or it needs to do another iteration (false)
   * @see [[kMeans.KMeansBuilder.endCondition]]
   */
  def endBySimilarity(counter: Int, previousCentroids: Array[Point], currentCentroids: Array[Point]): Boolean = {
    if (counter > 0) {
      (counter == max) || similarity(currentCentroids, previousCentroids, 50)
    } else {
      false
    }
  }

}
