package kMeans

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.DataLoader.loadSIFTs

import java.util.concurrent.TimeUnit

final case class KMeans(private val sc: SparkContext, private val nCentroids: Int, private val dataPath: String) {

  private var initCentroidSelector: Option[(RDD[SiftDescriptorContainer], Int) => Array[SiftDescriptorContainer]] = None

  private var mapReduce :Option[(RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer]] = None

  private var endCondition: Option[(Int, Array[SiftDescriptorContainer], Array[SiftDescriptorContainer]) => Boolean] = None

  def setInitCentroidSelector(ics: (RDD[SiftDescriptorContainer], Int) => Array[SiftDescriptorContainer]): Unit = {
    initCentroidSelector = Some(ics)
  }
  def setMapReduce(mr: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer]): Unit = {
    mapReduce = Some(mr)
  }

  def setEndCondition(ec: (Int, Array[SiftDescriptorContainer], Array[SiftDescriptorContainer]) => Boolean): Unit = {
    endCondition = Some(ec)
  }

  def compute(): Unit = {
    val startTime = System.nanoTime()
    val (result, iterations) = computation()
    val endTime = System.nanoTime()

    println("Computation completed in " + iterations + " iterations")
    println("Elapsed time: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms")
    //println(result.foreach(r => println(r.vector.mkString("Array(", ", ", ")"))))
  }

  private def computation() = {
    val rdd = loadSIFTs(sc, dataPath)
    var broadcastCentroids = sc.broadcast(initCentroidSelector.get(rdd, nCentroids))
    var iterations = 0
    var result = Array[SiftDescriptorContainer]()

    while(!endCondition.get(iterations, broadcastCentroids.value, result)) {
      if(iterations > 0){
        broadcastCentroids = sc.broadcast(result)
      }
      result = mapReduce.get(rdd, broadcastCentroids)
      broadcastCentroids.unpersist
      println("Iteration number " + iterations + " completed")
      iterations += 1
    }
    (result, iterations)
  }
}
