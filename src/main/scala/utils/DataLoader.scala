package utils

import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Utility object that contains methods used to load the data that will be analyzed by the program.
 */
object DataLoader {

  /**
   * This function is specificly designed to load a file of SIFT descriptors into an RDD.
   *
   * @param sc SparkContext of the master node.
   * @param fileName path to the file of SIFT descriptors.
   * @return RDD of all the SIFT's in the file (using the key as the SIFT's ID).
   */
  def loadSIFTs(sc: SparkContext, fileName: String): RDD[Point] = sc
      .sequenceFile(fileName, classOf[IntWritable], classOf[BytesWritable]) //data is serialized as (IntWritable, BytesWritable)
      .map(it => Point(it._1.get(), it._2.copyBytes())) //deep-copying data into a new Point
}
