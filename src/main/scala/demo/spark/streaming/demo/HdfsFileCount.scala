package demo.spark.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object HdfsFileCount {
  def main(args:Array[String]) {
    val conf = new SparkConf().setMaster("spark://sparkmaster:7077").setAppName("monitor hdfs")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://sparkmaster:9000/tmp/checkpoint")
    val lines = ssc.textFileStream("hdfs://sparkmaster:9000/eclipse/monitorhdfs")
    val result = lines.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
}