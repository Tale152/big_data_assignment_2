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


  def earlyHaltingMapReduce(data: RDD[Point], centroids: Broadcast[Array[Point]]): Array[Point] = {
    val lateChecksIndex: Int = data.take(1)(0).vector.length / 4
    data
      .map(point => {
        var minDist = Integer.MAX_VALUE
        val pointsDistance = centroids.value.map(centroid => {
          val pointCentroidDistance = distance(centroid, point, minDist, lateChecksIndex)
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

  def earlyHaltingUnravelingMapReduce(data: RDD[Point], centroids: Broadcast[Array[Point]]): Array[Point] = {
    val vectorLen = data.take(1)(0).vector.length
    var lateChecksIndex: Int = vectorLen / 4
    if(lateChecksIndex % 2 != 0){
      lateChecksIndex += 1 //ensuring that it is even
    }
    val isVectorLenOdd = vectorLen % 2 != 0
    data
      .map(point => {
        var minDist = Integer.MAX_VALUE
        val pointsDistance = centroids.value.map(centroid => {
          val pointCentroidDistance = distance(centroid, point, minDist, lateChecksIndex, isVectorLenOdd)
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

}
