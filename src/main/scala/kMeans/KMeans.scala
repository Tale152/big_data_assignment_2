package kMeans

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.DataLoader.loadSIFTs
import utils.Point

import java.util.concurrent.TimeUnit

/**
 * The structure that represents the K-means logical implementation, used in [[kMeans.KMeansBuilder]] to actually
 * build a KMeans implementation.
 */
trait KMeans {

  /**
   * Used to set a strategy which is going to specify how the initial centroids are going to be selected.
   * The strategy needed requires a Tuple(RDD[Point], Int), in which the RDD[Point] is the actual RDD
   * and Int is the number of centroids wanted.
   * The strategy returns an Array[Point] which are the centroids selected.
   */
  protected[kMeans] var initCentroidSelector: (RDD[Point], Int) => Array[Point]

  /**
   * Used to set a strategy that is going to specify how the points are going to be elaborated and how the new
   * set of centroids is going to be elected.
   * The strategy needed requires a Tuple(RDD[Point], Broadcast[Array[Point]]), in which the RDD[Point] is the actual
   * RDD and the Broadcast[Array[Point]] are centroids calculated in the previous iteration.
   * The strategy returns an Array[Point] which are the new centroids selected after all the points in the
   * RDD have been elaborated.
   */
  protected[kMeans] var mapReduce: (RDD[Point], Broadcast[Array[Point]]) => Array[Point]

  /**
   * Used to set a strategy that is going to specify the end conditions to stop the iteration of K-Means.
   * The strategy needed requires (Int, Array[Point], Array[Point]), in which Int is the max number of iteration,
   * the first Array[Point] is the array of centroids of the previous iteration and the other Array[Point] is
   * the array of centroids in the current iteration.
   * The strategy returns whether or not the K-Means algorithm must be ended.
   */
  protected[kMeans] var endCondition: (Int, Array[Point], Array[Point]) => Boolean

  /**
   * Used to specify the logical steps of the K-Means computation.
   */
  def compute() : Unit
}

object KMeans {

  def apply(sc: SparkContext, nCentroids: Int, dataPath: String): KMeans = KMeansImpl(sc, nCentroids, dataPath)

  private final case class KMeansImpl(sc: SparkContext, nCentroids: Int, dataPath: String) extends KMeans {

    override protected[kMeans] var initCentroidSelector: (RDD[Point], Int) => Array[Point] = _
    override protected[kMeans] var mapReduce: (RDD[Point], Broadcast[Array[Point]]) => Array[Point] = _
    override protected[kMeans] var endCondition: (Int, Array[Point], Array[Point]) => Boolean = _

    override def compute(): Unit = {
      val rdd = loadSIFTs(sc, dataPath).persist()
      val startTime = System.nanoTime()
      var broadcastCentroids = sc.broadcast(initCentroidSelector(rdd, nCentroids)) //broadcasting centroids to workers
      var iterations = 0
      var result = Array[Point]() //current centroids

      while(!endCondition(iterations, broadcastCentroids.value, result)) {
        //after the first iteration, broadcasting centroids calculated in the previous iteration
        if(iterations > 0){
          broadcastCentroids = sc.broadcast(result)
        }
        result = mapReduce(rdd, broadcastCentroids)
        broadcastCentroids.unpersist //removing centroids from the workers
        println("Iteration number " + iterations + " completed")
        iterations += 1
      }
      val endTime = System.nanoTime()
      rdd.unpersist() //removing RDD
      println("Computation completed in " + iterations + " iterations")
      println("Elapsed time: " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms")
      //println(result.foreach(r => println(r.vector.mkString("Array(", ", ", ")"))))
    }
  }
}
