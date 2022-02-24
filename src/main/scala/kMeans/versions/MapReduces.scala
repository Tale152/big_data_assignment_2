package kMeans.versions

import kMeans.EuclideanDistance.distance
import kMeans.VectorOperation.{divide, sum}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import utils.Point

object MapReduces {

  def baseMapReduce(data: RDD[Point], centroids: Broadcast[Array[Point]]): Array[Point] =
    data
      .map(point => {
        val pointsDistance = centroids.value.map(centroid => (centroid.id, distance(centroid, point)))
        val sortedPointsDistance = pointsDistance.sortBy(_._2)
        (sortedPointsDistance(0)._1, (1, point.vector.map(x => x.asInstanceOf[Double])))
      })
      .reduceByKey((x, y) => (x._1 + y._1, sum(x._2, y._2)))
      .map(v => Point(v._1, divide(v._2._2, v._2._1)))
      .collect()


  def earlyHaltingMapReduce(data: RDD[Point], centroids: Broadcast[Array[Point]]): Array[Point] =
    data
      .map(point => {
        var minDist = Integer.MAX_VALUE
        val pointsDistance = centroids.value.map(centroid => {
          val pointCentroidDistance = distance(centroid, point, minDist)
          if(pointCentroidDistance < minDist){
            minDist = pointCentroidDistance
          }
          (centroid.id, pointCentroidDistance)
        })
        val sortedPointsDistance = pointsDistance.sortBy(_._2)
        (sortedPointsDistance(0)._1, (1, point.vector.map(x => x.asInstanceOf[Double])))
      })
      .reduceByKey((x, y) => (x._1 + y._1, sum(x._2, y._2)))
      .map(v => Point(v._1, divide(v._2._2, v._2._1)))
      .collect()

}
