import org.apache.spark.{SparkConf, SparkContext}

object ContextFactory {

    def create(appName: String, masterAddress: String, jarPath: String) =
        new SparkContext(new SparkConf().setAppName(appName).setMaster(masterAddress).setJars(Seq(jarPath)))
}
