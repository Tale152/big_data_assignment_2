import org.apache.spark.SparkContext
import utils.{Const, ContextFactory}
import utils.LogEnabler.logSelectedOption
import kMeans.versions.BaseKMeans.{BaseKMeansCentroidsTermination, BaseKMeansIterationTermination}
import utils.ArgsProvider.{dataPath, logFlag, setArgs, sparkMaster, version}

object Main {

    def main(args: Array[String]): Unit = {
      setArgs(args)
      logSelectedOption(logFlag)
      val sc = ContextFactory.create(Const.appName, sparkMaster, Const.jarPath)
      kMeans(sc, dataPath, version)
      sc.stop()
    }

  private def kMeans(sc: SparkContext, dataPath: String, kMeansVersion: String): Unit = {
    println("Running K-Means version: " + kMeansVersion)
    kMeansVersion match {
      case "DEFAULT" => BaseKMeansIterationTermination().compute(sc, dataPath)
      case "CENTROID_STABILITY" => BaseKMeansCentroidsTermination().compute(sc, dataPath)
      case _ => throw new IllegalArgumentException("The specified k-means version does not exist")
    }
  }
}