package kMeans.versions

object BaseKMeans {
  case class BaseKMeansIterationTermination() extends KMeansRandomCentroids {
    private val iterationNumber = 10
    override protected final def endCondition(counter: Int): Boolean = counter == iterationNumber
  }
}
