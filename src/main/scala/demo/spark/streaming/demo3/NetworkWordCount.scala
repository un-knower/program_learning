package demo.spark.streaming.demo3

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
//for sortByKey
import org.apache.spark.SparkContext._
//run local[2] 1 localhost 9999
object NetworkWordCount {

  def main(args: Array[String]): Unit = {
//     if(args.length < 4){
//        System.err.println("usage: NetworkWordCount <master> <second> <hostname> <port>")
//        System.exit(1)
//     }
     
    StreamingExamples.setStreamingLogLevels()
     
    val sparkConf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999,StorageLevel.MEMORY_ONLY_SER)
    
    val words = lines.flatMap(_.split("\\s+"))
    
    val wordCounts = words.map(x => (x,1)).reduceByKey(_+_)

    val sortedWordCounts = wordCounts.map(x => (x._2,x._1)).transform(_.sortByKey(false)).map(x => (x._2,x._1))
    sortedWordCounts.foreachRDD(r => println("result:"+r.first.toString))
    ssc.start()
    ssc.awaitTermination()
    
  }

}