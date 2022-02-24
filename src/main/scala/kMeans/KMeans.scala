package kMeans

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.DataLoader.loadSIFTs
import utils.Point

import java.util.concurrent.TimeUnit

trait KMeans {
  protected[kMeans] var initCentroidSelector: (RDD[Point], Int) => Array[Point]
  protected[kMeans] var mapReduce: (RDD[Point], Broadcast[Array[Point]]) => Array[Point]
  protected[kMeans] var endCondition: (Int, Array[Point], Array[Point]) => Boolean

  def compute() : Unit
}

object KMeans {

  def apply(sc: SparkContext, nCentroids: Int, dataPath: String): KMeans = KMeansImpl(sc, nCentroids, dataPath)

  private final case class KMeansImpl(sc: SparkContext, nCentroids: Int, dataPath: String) extends KMeans {

    override protected[kMeans] var initCentroidSelector: (RDD[Point], Int) => Array[Point] = _
    override protected[kMeans] var mapReduce: (RDD[Point], Broadcast[Array[Point]]) => Array[Point] = _
    override protected[kMeans] var endCondition: (Int, Array[Point], Array[Point]) => Boolean = _

    override def compute(): Unit = {
      val rdd = loadSIFTs(sc, dataPath)
      val startTime = System.nanoTime()
      var broadcastCentroids = sc.broadcast(initCentroidSelector(rdd, nCentroids))
      var iterations = 0
      var result = Array[Point]()

      while(!endCondition(iterations, broadcastCentroids.value, result)) {
        if(iterations > 0){
          broadcastCentroids = sc.broadcast(result)
        }
        result = mapReduce(rdd, broadcastCentroids)
        broadcastCentroids.unpersist
        println("Iteration number " + iterations + " completed")
        iterations += 1
      }
      val endTime = System.nanoTime()
      println("Computation completed in " + iterations + " iterations")
      println("Elapsed time: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms")
      //println(result.foreach(r => println(r.vector.mkString("Array(", ", ", ")"))))
    }
  }
}
