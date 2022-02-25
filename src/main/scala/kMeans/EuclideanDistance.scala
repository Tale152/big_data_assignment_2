package kMeans

import utils.Point

/**
 * Utility object containing various implementations of the distance function that calculates the euclidean distance
 * between two points.
 */
object EuclideanDistance {

  /**
   * Calculates the euclidean distance between two points.
   * @param a first point
   * @param b second point
   * @return the calculated euclidean distance between a and b
   */
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

  /**
   * Calculates the euclidean distance between two points using the early halting technique.
   * @param a first point
   * @param b second point
   * @param currMin current minimum distance used to determine whether or not continue to calculate the distance between a and b
   * @param lateChecksIndex first n elements where the check against lateCheckIndex is not performed
   * @return the calculated euclidean distance between a and b or Integer.MAX_VALUE as effect of the early halting
   */
  def distance(a: Point, b: Point, currMin: Int, lateChecksIndex: Int): Int = {
    var ret = 0
    var i = 0
    while (i < lateChecksIndex) {
      //avoiding checks against curr min
      val t = a.vector(i) - b.vector(i)
      ret += t * t
      i = i + 1
    }
    while (i < a.vector.length) {
      //immediately returning Integer.MAX_VALUE if the current result is already bigger than currMin
      if(ret > currMin){
        return Integer.MAX_VALUE
      }
      val t = a.vector(i) - b.vector(i)
      ret += t * t
      i = i + 1
    }
    ret
  }

  /**
   * Calculates the euclidean distance between two points using the early halting and loop unravelling techniques.
   * @param a first point
   * @param b second point
   * @param currMin current minimum distance used to determine whether or not continue to calculate the distance between a and b
   * @param lateChecksIndex first n elements where the check against lateCheckIndex is not performed; needs to be even
   * @param isVectorLenOdd flag used internally for the loop unravelling
   * @return the calculated euclidean distance between a and b or Integer.MAX_VALUE as effect of the early halting
   */
  def distance(a: Point, b: Point, currMin: Int, lateChecksIndex: Int, isVectorLenOdd: Boolean): Int = {
    var ret = 0
    var i = 0
    while(i < lateChecksIndex){
      //avoiding checks against curr min and unravelling the loop executing two operations for every iteration
      val t1 = a.vector(i) - b.vector(i)
      val t2 = a.vector(i + 1) - b.vector(i + 1)
      ret += (t1 * t1) + (t2 * t2)
      i = i + 2 //assuming that lateChecksIndex is even we will reach it increasing the index by two every iteration
    }
    if(isVectorLenOdd){
      //if the length of the vector is odd an extra iteration is performed to avoid getting out of bound in the next loop
      val t = a.vector(i) - b.vector(i)
      ret += (t * t)
      i = i + 1
    }
    while (i < a.vector.length) {
      //immediately returning Integer.MAX_VALUE if the current result is already bigger than currMin
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
