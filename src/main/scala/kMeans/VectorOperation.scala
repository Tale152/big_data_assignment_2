package kMeans

object VectorOperation {
  def divide(vector: Array[Double], n: Int): Array[Byte] = vector.map(x => (x / n).asInstanceOf[Byte])

  def sum(x: Array[Double], y: Array[Double]): Array[Double] = {
    var i = 0
    val res = x.map(e => {
      val r = e + y(i)
      i += 1
      r
    })
    res
  }

}
