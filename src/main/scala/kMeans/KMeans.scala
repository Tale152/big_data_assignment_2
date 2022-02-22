package kMeans

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.DataLoader.loadSIFTs

trait KMeans {

  protected def initCentroidSelector(data: RDD[SiftDescriptorContainer]): Array[SiftDescriptorContainer]

  protected def mapReduce(data: RDD[SiftDescriptorContainer], centroids: Broadcast[Array[SiftDescriptorContainer]]):Array[SiftDescriptorContainer]

  protected def endCondition(counter: Int,
                             previousCentroids: Array[SiftDescriptorContainer],
                             currentCentroids: Array[SiftDescriptorContainer]): Boolean

  final def compute(sc: SparkContext, dataPath: String): Unit = {
    val rdd = loadSIFTs(sc, dataPath)
    var broadcastCentroids = sc.broadcast(initCentroidSelector(rdd))
    var iterations = 0
    var result = Array[SiftDescriptorContainer]()

    while(!endCondition(iterations, broadcastCentroids.value, result)) {
      if(iterations > 0){
        broadcastCentroids = sc.broadcast(result)
      }
      result = mapReduce(rdd, broadcastCentroids)
      broadcastCentroids.unpersist
      println("Iteration number " + iterations + " completed")
      iterations += 1
    }
    println("Computation completed in " + iterations + " iterations")
    println(result.foreach(r => println(r.vector.mkString("Array(", ", ", ")"))))
  }
}
