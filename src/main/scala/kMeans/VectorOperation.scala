package kMeans

import kMeans.EuclideanDistance.distance
import utils.Point

/**
 * Utility object containing operations on vectors.
 */
object VectorOperation {

  /**
   * Divides every element of an Array of Double by n and returns an array of Byte containing the results
   * @param vector the Array of double to divide
   * @param n the divisor
   * @return the resulting Array of Byte
   */
  def divide(vector: Array[Double], n: Int): Array[Byte] = vector.map(x => (x / n).asInstanceOf[Byte])

  /**
   * Takes two Arrays of Double and sums them element by element.
   * @param x first array in the sum
   * @param y second array in the sum
   * @return the new Array containing the sum of x and y
   */
  def sum(x: Array[Double], y: Array[Double]): Array[Double] = {
    var i = 0
    val res = x.map(e => {
      val r = e + y(i)
      i += 1
      r
    })
    res
  }

  /**
   * Confronts two Arrays of Point, checking every element of the vectors, and returns if they are similar (with a tolerance).
   * @param x first Array to confront
   * @param y second Array to confront
   * @param tolerance margin of tolerance used when confronting elements of the vectors
   * @return true if x and y's vectors are similar, false otherwise
   */
  def similarity(x: Array[Point], y: Array[Point], tolerance: Int): Boolean = {
    var i = 0
    x.forall(e => {
      val res = Math.abs(distance(e, y(i))) < tolerance
      i += 1
      res
    })
  }

}
