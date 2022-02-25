package kMeans

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Point

/**
 * Builder class used to create a [[KMeans]] in a concise way.
 */
case class KMeansBuilder() {

  private var path = Option.empty[String]
  private var sc = Option.empty[SparkContext]
  private var cn = Option.empty[Int]
  private var initSelector = Option.empty[(RDD[Point], Int) => Array[Point]]
  private var mr = Option.empty[(RDD[Point], Broadcast[Array[Point]]) => Array[Point]]
  private var ec = Option.empty[(Int, Array[Point], Array[Point]) => Boolean]

  /**
   * Sets the data path used by the [[KMeans]]
   * @param path the data path to use
   * @return this instance of KMeansBuilder
   */
  def dataPath(path: String): KMeansBuilder = {
    this.path = Option(path)
    this
  }

  /**
   * Sets the [[SparkContext]] used by the [[KMeans]]
   * @param sc the [[SparkContext]] to use
   * @return this instance of KMeansBuilder
   */
  def sparkContext(sc: SparkContext): KMeansBuilder = {
    this.sc = Option(sc)
    this
  }

  /**
   * Sets the number of centroids used by the [[KMeans]]
   * @param cn the number of centroids to use
   * @return this instance of KMeansBuilder
   */
  def centroidsNumber(cn: Int): KMeansBuilder = {
    this.cn = Option(cn)
    this
  }

  /**
   * Sets the centroid selector strategy used by the [[KMeans]]
   * @param initSelector the centroid selector strategy to use
   * @return this instance of KMeansBuilder
   */
  def initCentroidSelector(initSelector: (RDD[Point], Int) => Array[Point]): KMeansBuilder = {
    this.initSelector = Option(initSelector)
    this
  }

  /**
   * Sets the map-reduce strategy used by the [[KMeans]]
   * @param mr the map-reduce strategy to use
   * @return this instance of KMeansBuilder
   */
  def mapReduce(mr: (RDD[Point], Broadcast[Array[Point]]) => Array[Point]): KMeansBuilder = {
    this.mr = Option(mr)
    this
  }

  /**
   * Sets the end-condition strategy used by the [[KMeans]]
   * @param ec the end-condition strategy to use
   * @return this instance of KMeansBuilder
   */
  def endCondition(ec: (Int, Array[Point], Array[Point]) => Boolean): KMeansBuilder = {
    this.ec = Option(ec)
    this
  }

  /**
   * Builds the [[KMeans]] instance.
   * Needs to be invoked after setting [[centroidsNumber]], [[dataPath]], [[sparkContext]],
   * [[initCentroidSelector]], [[mapReduce]] and [[endCondition]].
   * @return the built instance of [[KMeans]]
   */
  def build(): KMeans = {
    val kMeans = KMeans(sc.get, cn.get, path.get)
    kMeans.initCentroidSelector = initSelector.get
    kMeans.mapReduce = mr.get
    kMeans.endCondition = ec.get
    kMeans
  }
}
