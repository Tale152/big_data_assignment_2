package kMeans

import eCP.Java.SiftDescriptorContainer

object EuclideanDistance {
  /* calc. Euclidean dist. between two points */
  def distance(a: SiftDescriptorContainer, b: SiftDescriptorContainer): Int = {
    var ret = 0
    var i = 0
    while (i < a.vector.length) {
      val t = a.vector(i) - b.vector(i)
      ret += t * t
      i = i + 1
    }
    ret
  }

  def distance(a: SiftDescriptorContainer, b: SiftDescriptorContainer, currMin: Int): Int = {
    var ret = 0
    var i = 0
    while (i < a.vector.length) {
      val t = a.vector(i) - b.vector(i)
      ret += t * t
      if(ret > currMin){
        return Integer.MAX_VALUE
      }
      i = i + 1
    }
    ret
  }
}
