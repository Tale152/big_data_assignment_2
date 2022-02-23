package kMeans.versions

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.rdd.RDD

object InitCentroidSelectors {

  def useFirstN(data: RDD[SiftDescriptorContainer], n: Int): Array[SiftDescriptorContainer] = data.take(n)

}
