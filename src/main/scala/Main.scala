import org.apache.spark.SparkContext

import utils.ContextFactory
import utils.LogEnabler.logSelectedOption
import utils.Const
import kMeans.versions.BaseKMeans.BaseKMeansIterationTermination

object Main {
    //val masterAddress = "spark://spark-VirtualBox:7077"
    val masterAddress = "spark://192.168.1.82:7077"
    val dataPath = "bigann_query.seq"

    def main(args: Array[String]){
      logSelectedOption(args)
      val sc = ContextFactory.create(Const.appName, masterAddress, Const.jarPath)
      kMeans(args, sc, dataPath)
      sc.stop()
    }

  private def kMeans(args: Array[String], sc: SparkContext, dataPath: String): Unit ={
    //TODO check args
    val kMeansVersion = "DEFAULT"
    kMeansVersion match {
      case "DEFAULT" => BaseKMeansIterationTermination().compute(sc, dataPath)
      case _ => throw new IllegalArgumentException("The specified k-means version does not exist")
    }
  }
}