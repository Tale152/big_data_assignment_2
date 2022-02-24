package kMeans

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Point

case class KMeansBuilder() {

  private var path = Option.empty[String]
  private var sc = Option.empty[SparkContext]
  private var cn = Option.empty[Int]
  private var initSelector = Option.empty[(RDD[Point], Int) => Array[Point]]
  private var mr = Option.empty[(RDD[Point], Broadcast[Array[Point]]) => Array[Point]]
  private var ec = Option.empty[(Int, Array[Point], Array[Point]) => Boolean]

  def dataPath(path: String): KMeansBuilder = {
    this.path = Option(path)
    this
  }

  def sparkContext(sc: SparkContext): KMeansBuilder = {
    this.sc = Option(sc)
    this
  }

  def centroidsNumber(cn: Int): KMeansBuilder = {
    this.cn = Option(cn)
    this
  }

  def initCentroidSelector(initSelector: (RDD[Point], Int) => Array[Point]): KMeansBuilder = {
    this.initSelector = Option(initSelector)
    this
  }

  def mapReduce(mr: (RDD[Point], Broadcast[Array[Point]]) => Array[Point]): KMeansBuilder = {
    this.mr = Option(mr)
    this
  }

  def endCondition(ec: (Int, Array[Point], Array[Point]) => Boolean): KMeansBuilder = {
    this.ec = Option(ec)
    this
  }

  def build(): KMeans = {
    val kMeans = KMeans(sc.get, cn.get, path.get)
    kMeans.initCentroidSelector = initSelector.get
    kMeans.mapReduce = mr.get
    kMeans.endCondition = ec.get
    kMeans
  }

}
