package kMeans.versions

import kMeans.EuclideanDistance.distance
import kMeans.VectorOperation.{divide, sum}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Point

/**
 * Object that contains various implementation for the map-reduce strategy to provide to
 * [[kMeans.KMeansBuilder]] as argument in the [[kMeans.KMeansBuilder.mapReduce]] method.
 */
object MapReduces {

  /**
   * Basic implementation of the map-reduce strategy used in the [[kMeans.KMeans.compute]]
   * @param data source of RDD to execute K-Means upon
   * @param centroids broadcasted Array of [[Point]] containing the centroids used in the K-Means algorithm
   * @return Array of [[Point]] containing the centroids calculated by K-means
   */
  def baseMapReduce(data: RDD[Point], centroids: Broadcast[Array[Point]]): Array[Point] =
    data
      .map(point => {
        val pointsDistance = centroids.value.map(centroid => (centroid.id, distance(centroid, point)))
        (pointsDistance.minBy(_._2)._1, (1, point.vector.map(x => x.asInstanceOf[Double])))
      })
      .reduceByKey((x, y) => (x._1 + y._1, sum(x._2, y._2)))
      .map(v => Point(v._1, divide(v._2._2, v._2._1)))
      .collect()

  /**
   * Implementation of the map-reduce strategy used in the [[kMeans.KMeans.compute]] that uses the early halting technique
   * in order to reduce execution time.
   * @param data source of RDD to execute K-Means upon
   * @param centroids broadcasted Array of [[Point]] containing the centroids used in the K-Means algorithm
   * @return Array of [[Point]] containing the centroids calculated by K-means
   */
  def earlyHaltingMapReduce(data: RDD[Point], centroids: Broadcast[Array[Point]]): Array[Point] = {
    //number of elements where the early halting check will not be executed
    val lateChecksIndex: Int = data.take(1)(0).vector.length / 4
    data
      .map(point => {
        var minDist = Integer.MAX_VALUE //initializing minDist that will be used as reference for the early halting
        val pointsDistance = centroids.value.map(centroid => {
          val pointCentroidDistance = distance(centroid, point, minDist, lateChecksIndex) //distance calculation
          if(pointCentroidDistance < minDist){
            minDist = pointCentroidDistance //set the new minDist that will be used as reference for the early halting
          }
          (centroid.id, pointCentroidDistance)
        })
        (pointsDistance.minBy(_._2)._1, (1, point.vector.map(x => x.asInstanceOf[Double])))
      })
      .reduceByKey((x, y) => (x._1 + y._1, sum(x._2, y._2)))
      .map(v => Point(v._1, divide(v._2._2, v._2._1)))
      .collect()
  }
  /**
   * Implementation of the map-reduce strategy used in the [[kMeans.KMeans.compute]] that uses the early halting technique
   * and the loop unraveling technique in order to reduce execution time.
   * @param data source of RDD to execute K-Means upon
   * @param centroids broadcasted Array of [[Point]] containing the centroids used in the K-Means algorithm
   * @return Array of [[Point]] containing the centroids calculated by K-means
   */
  def earlyHaltingUnravelingMapReduce(data: RDD[Point], centroids: Broadcast[Array[Point]]): Array[Point] = {
    //length of a vector inside a Point
    val vectorLen = data.take(1)(0).vector.length
    //number of elements where the early halting check will not be executed
    var lateChecksIndex: Int = vectorLen / 4
    if(lateChecksIndex % 2 != 0){
      lateChecksIndex += 1 //ensuring that it is even, required by the distance method
    }
    //used in the distance method to decide if doing or not an extra iteration
    val isVectorLenOdd = vectorLen % 2 != 0
    data
      .map(point => {
        var minDist = Integer.MAX_VALUE //initializing minDist that will be used as reference for the early halting
        val pointsDistance = centroids.value.map(centroid => {
          val pointCentroidDistance = distance(centroid, point, minDist, lateChecksIndex, isVectorLenOdd) //distance calculation
          if(pointCentroidDistance < minDist){
            minDist = pointCentroidDistance //set the new minDist that will be used as reference for the early halting
          }
          (centroid.id, pointCentroidDistance)
        })
        (pointsDistance.minBy(_._2)._1, (1, point.vector.map(x => x.asInstanceOf[Double])))
      })
      .reduceByKey((x, y) => (x._1 + y._1, sum(x._2, y._2)))
      .map(v => Point(v._1, divide(v._2._2, v._2._1)))
      .collect()
  }

}
