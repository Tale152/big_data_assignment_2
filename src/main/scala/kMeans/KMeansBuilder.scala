package kMeans

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

case class KMeansBuilder() {

  private var path = Option.empty[String]
  private var sc = Option.empty[SparkContext]
  private var cn = Option.empty[Int]
  private var initSelector = Option.empty[(RDD[SiftDescriptorContainer], Int) => Array[SiftDescriptorContainer]]
  private var mr = Option.empty[(RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer]]
  private var ec = Option.empty[(Int, Array[SiftDescriptorContainer], Array[SiftDescriptorContainer]) => Boolean]

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

  def initCentroidSelector(initSelector: (RDD[SiftDescriptorContainer], Int) => Array[SiftDescriptorContainer]): KMeansBuilder = {
    this.initSelector = Option(initSelector)
    this
  }

  def mapReduce(mr: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer]): KMeansBuilder = {
    this.mr = Option(mr)
    this
  }

  def endCondition(ec: (Int, Array[SiftDescriptorContainer], Array[SiftDescriptorContainer]) => Boolean): KMeansBuilder = {
    this.ec = Option(ec)
    this
  }

  def build(): KMeans = {
    //TODO check no Option.empty
    val kMeans = KMeans(sc.get, cn.get, path.get)
    kMeans.initCentroidSelector = initSelector.get
    kMeans.mapReduce = mr.get
    kMeans.endCondition = ec.get
    kMeans
  }

}
