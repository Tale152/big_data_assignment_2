package kMeans

import eCP.Java.SiftDescriptorContainer
import kMeans.EuclideanDistance.distance

object VectorOperation {

  def divide(vector: Array[Double], n: Int): Array[Byte] = vector.map(x => (x / n).asInstanceOf[Byte])

  def sum(x: Array[Double], y: Array[Double]): Array[Double] = (x,y).zipped.map((a,b) => a + b)

  def similarity(x: Array[SiftDescriptorContainer], y: Array[SiftDescriptorContainer], tolerance: Int): Boolean =
    (x,y).zipped.forall((a,b) => Math.abs(distance(a,b)) < tolerance)

}
