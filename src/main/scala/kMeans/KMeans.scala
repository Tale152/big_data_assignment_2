package kMeans

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.DataLoader.loadSIFTs

protected trait KMeans {

  def initCentroidSelector(data: RDD[SiftDescriptorContainer]): Array[SiftDescriptorContainer]

  def mapReduce(data: RDD[SiftDescriptorContainer], centroids: Broadcast[Array[SiftDescriptorContainer]]):Array[SiftDescriptorContainer]

  def endCondition(counter: Int): Boolean

  final def compute(sc: SparkContext, dataPath: String): Array[SiftDescriptorContainer] = {
    val rdd = loadSIFTs(sc, dataPath)
    var broadcastCentroids = sc.broadcast(initCentroidSelector(rdd))
    var iterations = 0
    var result = Array[SiftDescriptorContainer]()

    while(!endCondition(iterations)) {
      if(iterations != 0){
        broadcastCentroids = sc.broadcast(result)
      }
      result = mapReduce(rdd, broadcastCentroids)
      broadcastCentroids.unpersist
      iterations += 1
    }
    result
  }
}
