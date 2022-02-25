package kMeans.versions

import org.apache.spark.rdd.RDD
import utils.Point

/**
 * Object that contains various implementation for the init centroid selector strategy to provide to
 * [[kMeans.KMeansBuilder]] as argument in the [[kMeans.KMeansBuilder.initCentroidSelector]] method.
 */
object InitCentroidSelectors {

  /**
   * Selects the first n elements of the provided data.
   * @param data RDD of [[Point]] that contains all the Data to analyze and from where the centroid will be chosen
   * @param n number of centroids to take
   * @return an Array of [[Point]] containing the chosen centroids
   */
  def useFirstN(data: RDD[Point], n: Int): Array[Point] = data.take(n)

  /**
   * Selects n elements from the provided data selecting them every (data.count / n) elements.
   * @param data RDD of [[Point]] that contains all the Data to analyze and from where the centroid will be chosen
   * @param n number of centroids to take
   * @return an Array of [[Point]] containing the chosen centroids
   */
  def useEvenlySpaced(data: RDD[Point], n: Int): Array[Point] = {
    val gap:Int = Math.floor(data.count().toDouble / n.toDouble).toInt
    val dataArray = data.collect
    (for (i <- 0 until n) yield dataArray(i * gap)).toArray
  }
}
