package kMeans.versions

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.rdd.RDD

object InitCentroidSelectors {

  def useFirstN(data: RDD[SiftDescriptorContainer], n: Int): Array[SiftDescriptorContainer] = data.take(n)

  def useEvenlySpaced(data: RDD[SiftDescriptorContainer], n: Int): Array[SiftDescriptorContainer] = {
    val gap:Int = Math.floor(data.count().toDouble / n.toDouble).toInt
    val dataArray = data.collect
    (for (i <- 0 until n) yield dataArray(i * gap)).toArray
  }

}
