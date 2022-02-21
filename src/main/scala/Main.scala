import org.apache.spark.{SparkConf, SparkContext}
import eCP.Java.SiftDescriptorContainer

object Main {
    
    val appName = "test"
    val masterAddress = "spark://192.168.1.82:7077"
    val jarPath = "target\\scala-2.12\\app_2.12-1.0.jar"

    val centroidNumber = 10

    def main(args: Array[String]){

        println("STARTING COMPUTATION")
        val sc = ContextFactory.create(appName, masterAddress, jarPath)

        val rdd = DataLoader.loadSIFTs(sc, "C:\\Users\\teemo\\Desktop\\big_data_assignment_2\\bigann_query.seq")
        
        val randomCentroids = rdd.take(centroidNumber)

        val broadcastCentroids = sc.broadcast(randomCentroids)
        broadcastCentroids.value.map(point => println(point))

        val distRDD = rdd.map(point => {
            import EuclideanDistance.distance

	        val pointsDistance = broadcastCentroids.value.map(centroid => (centroid.id, distance(centroid, point)))
	        val sortedPointsDistance = pointsDistance.sortBy(_._2)
	        (sortedPointsDistance(0)._1, 1)
        })

        val results = distRDD.reduceByKey( _ + _ ).collect

        results.sortBy(_._1).map(println)

        broadcastCentroids.unpersist

        sc.stop()
        println("COMPUTATION ENDED")
    }
}