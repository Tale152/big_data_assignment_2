import kMeans.implementations.BaseKMeans
import org.apache.log4j.{Level, Logger}
import utils.ContextFactory
import java.nio.file.Paths


object Main {

    val appName = "test"
    val jarPath: String = Paths.get("target", "scala-2.12", "app_2.12-1.0.jar").toString

    //val masterAddress = "spark://spark-VirtualBox:7077"
    val masterAddress = "spark://192.168.1.82:7077"
    val dataPath = "bigann_query.seq"

    def main(args: Array[String]){
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)

      println("STARTING COMPUTATION")

      val sc = ContextFactory.create(appName, masterAddress, jarPath)

      val result = BaseKMeans().compute(sc, dataPath)

      println(result.mkString("Array(", ", ", ")"))

      sc.stop()
      println("COMPUTATION ENDED")
    }
}