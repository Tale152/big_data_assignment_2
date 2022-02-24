package kMeans

import utils.Point

object EuclideanDistance {

  /* calc. Euclidean dist. between two points */
  def distance(a: Point, b: Point): Int = {
    var ret = 0
    var i = 0
    while (i < a.vector.length) {
      val t = a.vector(i) - b.vector(i)
      ret += t * t
      i = i + 1
    }
    ret
  }

  def distance(a: Point, b: Point, currMin: Int, lateChecksIndex: Int): Int = {
    var ret = 0
    var i = 0
    while (i < lateChecksIndex) {
      val t = a.vector(i) - b.vector(i)
      ret += t * t
      i = i + 1
    }
    while (i < a.vector.length) {
      if(ret > currMin){
        return Integer.MAX_VALUE
      }
      val t = a.vector(i) - b.vector(i)
      ret += t * t
      i = i + 1
    }
    ret
  }

  def distance(a: Point, b: Point, currMin: Int, lateChecksIndex: Int, isVectorLenOdd: Boolean): Int = {
    var ret = 0
    var i = 0
    while(i < lateChecksIndex){
      val t1 = a.vector(i) - b.vector(i)
      val t2 = a.vector(i + 1) - b.vector(i + 1)
      ret += (t1 * t1) + (t2 * t2)
      i = i + 2
    }
    if(isVectorLenOdd){
      val t = a.vector(i) - b.vector(i)
      ret += (t * t)
      i = i + 1
    }
    while (i < a.vector.length) {
      if(ret > currMin){
        return Integer.MAX_VALUE
      }
      val t1 = a.vector(i) - b.vector(i)
      val t2 = a.vector(i + 1) - b.vector(i + 1)
      ret += (t1 * t1) + (t2 * t2)
      i = i + 2
    }
    ret
  }
}
