package kMeans

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

case class KMeansBuilder() {

  private var path = Option.empty[String]
  private var sc = Option.empty[SparkContext]
  private var initSelector = Option.empty[RDD[SiftDescriptorContainer] => Array[SiftDescriptorContainer]]
  private var mr = Option.empty[(RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer]]
  private var ec = Option.empty[Int => Boolean]

  def dataPath(path: String): KMeansBuilder = {
    this.path = Option(path)
    this
  }

  def sparkContext(sc: SparkContext): KMeansBuilder = {
    this.sc = Option(sc)
    this
  }

  def initCentroidSelector(initSelector: RDD[SiftDescriptorContainer] => Array[SiftDescriptorContainer]): KMeansBuilder = {
    this.initSelector = Option(initSelector)
    this
  }

  def mapReduce(mr: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer]): KMeansBuilder = {
    this.mr = Option(mr)
    this
  }

  def endCondition(ec: Int => Boolean): KMeansBuilder = {
    this.ec = Option(ec)
    this
  }

  def build(): KMeansComputation = {
    //TODO check no Option.empty
    KMeansComputation(sc.get, path.get, initSelector.get, mr.get, ec.get)
  }

}
