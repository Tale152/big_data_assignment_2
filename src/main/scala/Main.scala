import EuclideanDistance.distance
import eCP.Java.SiftDescriptorContainer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object Main {
    
    val appName = "test"
    val masterAddress = "spark://192.168.1.82:7077"
    val jarPath = "target\\scala-2.12\\app_2.12-1.0.jar"

    val centroidNumber = 10
    val iterationNumber = 20

    def main(args: Array[String]){

        println("STARTING COMPUTATION")
        val sc = ContextFactory.create(appName, masterAddress, jarPath)

       val kMeans = KMeansBuilder()
          .sparkContext(sc)
          .dataPath("C:\\Users\\teemo\\Desktop\\big_data_assignment_2\\bigann_query.seq")
          .initCentroidSelector(rdd => rdd.take(centroidNumber))
          .mapReduce(mr)
          .endCondition(iteration => iteration == iterationNumber)
          .build()

        val result = kMeans.compute

        println(result.length)

        sc.stop()
        println("COMPUTATION ENDED")
    }

  def sumArray(x: Array[Byte], y: Array[Byte]): Array[Int] ={
    var i = 0
    x.map(a => {

      val res = a + y(i)
      i += 1
      res
    })
  }

  private case class IdGenerator(){
    private var i = 0
    def getNextId: Int = {
      val res = i
      i += 1
      res
    }
  }

  def divideVector(vector: Array[Double], n: Int): Array[Byte] = vector.map(x => (x/n).asInstanceOf[Byte])

  def sumVectors(x: Array[Double], y: Array[Double]): Array[Double] = {
    var i = 0
    x.map(e => {
      val r = e + y(i)
      i += 1
      r
    })
  }

  def mr: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer] = {
    val idGenerator = IdGenerator()
    (rdd, broadcastCentroids) => rdd
      .map(point => {
        val pointsDistance = broadcastCentroids.value.map(centroid => (centroid.id, distance(centroid, point)))
        val sortedPointsDistance = pointsDistance.sortBy(_._2)
        (sortedPointsDistance(0)._1, (1, point.vector.map(x => x.asInstanceOf[Double])))
      })
      .reduceByKey((x,y) => (x._1 + y._1, sumVectors(x._2, y._2)))
      .map(v => new SiftDescriptorContainer(idGenerator.getNextId, divideVector(v._2._2, v._2._1)))
      .collect()
  }

}