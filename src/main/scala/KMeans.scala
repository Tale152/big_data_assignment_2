import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


trait KMeans {
  def compute(sc: SparkContext, dataPath: String): Array[SiftDescriptorContainer]
}

protected abstract class AbstractKMeans extends KMeans {

  def initCentroidSelector(data: RDD[SiftDescriptorContainer]): Array[SiftDescriptorContainer]

  def mapReduce(data: RDD[SiftDescriptorContainer], centroids: Broadcast[Array[SiftDescriptorContainer]]):Array[SiftDescriptorContainer]

  def endCondition(counter: Int): Boolean

  final def compute(sc: SparkContext, dataPath: String): Array[SiftDescriptorContainer] = KMeansBuilder()
      .sparkContext(sc)
      .dataPath(dataPath)
      .initCentroidSelector(initCentroidSelector)
      .mapReduce(mapReduce)
      .endCondition(endCondition)
      .build()
      .compute
}
