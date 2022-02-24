package kMeans

import eCP.Java.SiftDescriptorContainer

object EuclideanDistance {

  /* calc. Euclidean dist. between two points */
  def distance(a: SiftDescriptorContainer, b: SiftDescriptorContainer): Int =
    (a.vector, b.vector).zipped.map((x,y) => x - y).foldLeft(0)((accumulator,v) => accumulator + (v * v))

  def distance(a: SiftDescriptorContainer, b: SiftDescriptorContainer, currMin: Int): Int = {
    var ret: Int = 0
    for((x,y) <- (a.vector, b.vector).zipped){
      val t = x - y
      ret += t * t
      if(ret > currMin){
        return Integer.MAX_VALUE
      }
    }
    ret
  }
}
