package kMeans

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.DataLoader.loadSIFTs

import java.util.concurrent.TimeUnit

trait KMeans {

  protected def initCentroidSelector(data: RDD[SiftDescriptorContainer], n: Int): Array[SiftDescriptorContainer]

  protected def mapReduce(data: RDD[SiftDescriptorContainer],
                          centroids: Broadcast[Array[SiftDescriptorContainer]]): Array[SiftDescriptorContainer]

  protected def endCondition(counter: Int,
                             previousCentroids: Array[SiftDescriptorContainer],
                             currentCentroids: Array[SiftDescriptorContainer]): Boolean

  final def compute(sc: SparkContext, nCentroids: Int, dataPath: String): Unit = {
    val startTime = System.nanoTime()
    val (result, iterations) = computation(sc, nCentroids, dataPath)
    val endTime = System.nanoTime()

    println("Computation completed in " + iterations + " iterations")
    println("Elapsed time: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms")
    //println(result.foreach(r => println(r.vector.mkString("Array(", ", ", ")"))))
  }

  private def computation(sc: SparkContext, nCentroids: Int, dataPath: String): (Array[SiftDescriptorContainer], Int) = {
    val rdd = loadSIFTs(sc, dataPath)
    var broadcastCentroids = sc.broadcast(initCentroidSelector(rdd, nCentroids))
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
    (result, iterations)
  }
}
