import org.apache.spark.SparkContext
import utils.{Const, ContextFactory}
import utils.LogEnabler.logSelectedOption
import kMeans.versions.BaseKMeans.{BaseKMeansCentroidsTermination, BaseKMeansIterationTermination}
import kMeans.versions.KMeansVersions
import utils.ArgsProvider.{dataPath, logFlag, setArgs, sparkMaster, version, nCentroids}

object Main {

    def main(args: Array[String]): Unit = {
      setArgs(args)
      logSelectedOption(logFlag)
      val sc = ContextFactory.create(Const.appName, sparkMaster, Const.jarPath)
      kMeans(sc)
      sc.stop()
    }

  private def kMeans(sc: SparkContext): Unit = {
    println("Running K-Means version: " + version + " with " + nCentroids + " centroids")
    version match {
      case KMeansVersions.DEFAULT => BaseKMeansIterationTermination().compute(sc, nCentroids, dataPath)
      case KMeansVersions.CENTROID_STABILITY => BaseKMeansCentroidsTermination().compute(sc, nCentroids, dataPath)
      case _ => throw new IllegalArgumentException("The specified k-means version does not exist")
    }
  }
}