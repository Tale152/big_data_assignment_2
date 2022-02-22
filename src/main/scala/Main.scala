import eCP.Java.SiftDescriptorContainer
import kMeans.implementations.BaseKMeans
import org.apache.spark.SparkContext
import utils.ContextFactory

import java.nio.file.Paths
import utils.LogEnabler.logSelectedOption


object Main {

    val appName = "test"
    val jarPath: String = Paths.get("target", "scala-2.12", "app_2.12-1.0.jar").toString

    //val masterAddress = "spark://spark-VirtualBox:7077"
    val masterAddress = "spark://192.168.1.82:7077"
    val dataPath = "bigann_query.seq"

    def main(args: Array[String]){
      logSelectedOption(args)
      val sc = ContextFactory.create(appName, masterAddress, jarPath)
      kMeans(args, sc, dataPath)
      sc.stop()
    }

  private def kMeans(args: Array[String], sc: SparkContext, dataPath: String): Unit ={
    //TODO check args
    val kMeansVersion = "DEFAULT"
    val result = kMeansVersion match {
      case "DEFAULT" => BaseKMeans().compute(sc, dataPath)
      case _ => throw new IllegalArgumentException("The specified k-means version does not exist")
    }
    println(result.foreach(r => println(r.vector.mkString("Array(", ", ", ")"))))
  }
}