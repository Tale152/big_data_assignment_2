object main {
    
    val appName = "test"
    val masterAddress = "spark://192.168.1.82:7077"
    val jarPath = "..\\workers\\workers.jar"

    def main(args: Array[String]){

        println("STARTING COMPUTATION")
        val sc = ContextFactory.create(appName, masterAddress, jarPath)
        sc.stop()
        println("COMPUTATION ENDED")
    }
}