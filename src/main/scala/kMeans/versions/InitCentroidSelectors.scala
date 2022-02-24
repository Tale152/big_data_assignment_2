package kMeans.versions

import org.apache.spark.rdd.RDD
import utils.Point

object InitCentroidSelectors {

  def useFirstN(data: RDD[Point], n: Int): Array[Point] = data.take(n)

  def useEvenlySpaced(data: RDD[Point], n: Int): Array[Point] = {
    val gap:Int = Math.floor(data.count().toDouble / n.toDouble).toInt
    val dataArray = data.collect
    (for (i <- 0 until n) yield dataArray(i * gap)).toArray
  }

}
