package demo.spark.streaming.demo3

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
//for sortByKey
import org.apache.spark.SparkContext._

//run local[2] 1 localhost 9999 20 10
object WindowWordCount {
  def main(args: Array[String]): Unit = {
     if(args.length < 6){
        System.err.println("usage:  <master> <second> <hostname> <port> <windowDuration> <slideInternal>")
        System.exit(1)
     }
     
    StreamingExamples.setStreamingLogLevels()
     
    val sparkConf = new SparkConf().setAppName("WindowWordCount").setMaster(args(0))
    val ssc = new StreamingContext(sparkConf, Seconds(args(1).toInt))
    ssc.checkpoint(".")
    val lines = ssc.socketTextStream(args(2),args(3).toInt,StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x,1)).reduceByKeyAndWindow(_+_,_-_,Seconds(args(4).toInt),Seconds(args(5).toInt))
    val sortedWordCounts = wordCounts.map(x => (x._2,x._1)).transform(_.sortByKey(false)).map(x => (x._2,x._1))
    sortedWordCounts.foreachRDD(r => println("result:"+r.first.toString))
    ssc.start()
    ssc.awaitTermination()
    
  }
}