import eCP.Java.SiftDescriptorContainer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

trait KMeansComputation {
    def compute: Array[SiftDescriptorContainer]
}

object KMeansComputation {

    def apply(sc: SparkContext,
                 path: String,
                 initCentroidSelector: RDD[SiftDescriptorContainer] => Array[SiftDescriptorContainer],
                 mapReduce: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer],
                 endCondition: Int => Boolean): KMeansComputationImpl ={
        new KMeansComputationImpl(sc, path, initCentroidSelector, mapReduce, endCondition)
    }

    class KMeansComputationImpl(sc: SparkContext,
                                path: String,
                                initCentroidSelector: RDD[SiftDescriptorContainer] => Array[SiftDescriptorContainer],
                                mapReduce: (RDD[SiftDescriptorContainer], Broadcast[Array[SiftDescriptorContainer]]) => Array[SiftDescriptorContainer],
                                endCondition: Int => Boolean) extends KMeansComputation {
        override def compute: Array[SiftDescriptorContainer] = {
            val rdd = DataLoader.loadSIFTs(sc, path)
            var broadcastCentroids = sc.broadcast(initCentroidSelector(rdd))
            var iterations = 0
            var result = Array[SiftDescriptorContainer]()

            while(!endCondition(iterations)) {
                if(iterations != 0){
                    broadcastCentroids = sc.broadcast(result)
                }
                result = mapReduce(rdd, broadcastCentroids)
                broadcastCentroids.unpersist
                iterations += 1
            }
            result
        }
    }

}