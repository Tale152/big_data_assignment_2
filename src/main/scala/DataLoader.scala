import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.IntWritable

object DataLoader {

    /** 
    * This function is specificly designed to load a file of SIFT descriptors into an RDD. 
    * @param sc SparkContext of the master node. 
    * @param fileName path to the file of SIFT descriptors.
    * @return RDD of all the SIFT's in the file (using the key as the SIFT's ID). 
    */
    def loadSIFTs(sc: SparkContext, fileName: String): RDD[SiftDescriptorContainer] = {
        //We use the spark context to read the data from a HDFS sequence file, but we need to 
        //load the hadoop class for the key (IntWriteable) and map it to the ID
        sc.sequenceFile(fileName, classOf[IntWritable], classOf[SiftDescriptorContainer]).map(it => {
            //first we map to avoid HDFS reader "re-use-of-writable" issue
            val desc: SiftDescriptorContainer = new SiftDescriptorContainer()
            desc.id = it._2.id
            //DeepCopy needed as the Java HDFS reader re-uses the memory
            it._2.vector.copyToArray(desc.vector)
            desc
        })
    }
}