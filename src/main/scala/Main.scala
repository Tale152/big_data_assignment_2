import org.apache.spark.{SparkConf, SparkContext}

object Main {
    
    val appName = "test"
    val masterAddress = "spark://192.168.1.82:7077"
    val jarPath = "target\\scala-2.12\\app_2.12-1.0.jar"

    def main(args: Array[String]){

        println("STARTING COMPUTATION")
        val sc = ContextFactory.create(appName, masterAddress, jarPath)

        val rdd = DataLoader.loadSIFTs(sc, "C:\\Users\\teemo\\Desktop\\big_data_assignment_2\\bigann_query.seq")
        println(rdd.count)

        sc.stop()
        println("COMPUTATION ENDED")
    }
}