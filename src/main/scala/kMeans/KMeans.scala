package kMeans

import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.DataLoader.loadSIFTs

import java.util.concurrent.TimeUnit

trait KMeans {
  protected[kMeans] var initCentroidSelector: (RDD[SiftDescriptorContainer], Int) => Array[SiftDescriptorContainer]
  protected[kMeans] var mapReduce: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer]
  protected[kMeans] var endCondition: (Int, Array[SiftDescriptorContainer], Array[SiftDescriptorContainer]) => Boolean

  def compute() : Unit
}

object KMeans {

  def apply(sc: SparkContext, nCentroids: Int, dataPath: String): KMeans = KMeansImpl(sc, nCentroids, dataPath)

  private final case class KMeansImpl(sc: SparkContext, nCentroids: Int, dataPath: String) extends KMeans {

    override protected[kMeans] var initCentroidSelector: (RDD[SiftDescriptorContainer], Int) => Array[SiftDescriptorContainer] = _
    override protected[kMeans] var mapReduce: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer] = _
    override protected[kMeans] var endCondition: (Int, Array[SiftDescriptorContainer], Array[SiftDescriptorContainer]) => Boolean = _

    override def compute(): Unit = {
      val startTime = System.nanoTime()
      val (_, iterations) = computation()
      val endTime = System.nanoTime()

      println("Computation completed in " + iterations + " iterations")
      println("Elapsed time: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms")
      //println(result.foreach(r => println(r.vector.mkString("Array(", ", ", ")"))))
    }

    private def computation() = {
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
}

