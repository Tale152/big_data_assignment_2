package utils

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Utility object that provides a method to create a [[SparkContext]]
 */
object ContextFactory {

    /**
     * @param appName name that will be used for the computation in the Spark cluster
     * @param masterAddress address of the master that coordinates the Spark cluster
     * @param jarPath path where the Jar of this program (used by the Spark cluster's Workers) is stored
     * @return the newly created [[SparkContext]]
     */
    def create(appName: String, masterAddress: String, jarPath: String): SparkContext = new SparkContext(
        new SparkConf().setAppName(appName).setMaster(masterAddress).setJars(Seq(jarPath))
    )
}
