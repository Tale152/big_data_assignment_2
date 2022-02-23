import org.apache.spark.SparkContext
import utils.ContextFactory
import utils.LogEnabler.logSelectedOption
import utils.Const
import kMeans.versions.BaseKMeans.{BaseKMeansCentroidsTermination, BaseKMeansIterationTermination}

import java.io.File

object Main {

    def main(args: Array[String]): Unit = {
      checkArgs(args)
      logSelectedOption(args)
      val sc = ContextFactory.create(Const.appName, args.find(s => s.contains("spark://")).get, Const.jarPath)
      kMeans(args, sc, args.find(s => s.contains(".seq")).get)
      sc.stop()
    }

    private def checkArgs(args: Array[String]): Unit = {
        if(args.count(s => s.contains("spark://")) != 1){
            throw new IllegalArgumentException("Something went wrong with the specified Spark Master address")
        } else if (args.count(s => s.contains(".seq")) != 1){
            throw new IllegalArgumentException("Something went wrong with the specified .seq file")
        } else if (!(new File(args.find(s => s.contains(".seq")).get).exists())){
            throw new IllegalStateException(args.find(s => s.contains(".seq")).get + " does not exist")
        }
    }

  private def kMeans(args: Array[String], sc: SparkContext, dataPath: String): Unit ={
    //TODO check args
    val kMeansVersion = "CENTROID_STABILITY"
    kMeansVersion match {
      case "DEFAULT" => BaseKMeansIterationTermination().compute(sc, dataPath)
      case "CENTROID_STABILITY" => BaseKMeansCentroidsTermination().compute(sc, dataPath)
      case _ => throw new IllegalArgumentException("The specified k-means version does not exist")
    }
  }
}