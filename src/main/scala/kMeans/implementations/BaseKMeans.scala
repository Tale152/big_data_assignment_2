package kMeans.implementations

import kMeans.KMeansRandomCentroids

object BaseKMeans {
  case class BaseKMeansIterationTermination() extends KMeansRandomCentroids {
    private val iterationNumber = 10
    override def endCondition(counter: Int): Boolean = counter == iterationNumber
  }
}
