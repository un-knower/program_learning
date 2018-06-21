

package demo.spark.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
/**
 * 读取hdfs文件 
 */
object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("").setMaster("local[2]")
    //StreamingContext is the main entry point for all streaming functionality
    val ssc = new StreamingContext(sparkConf,Seconds(5))  //设置5秒一个批次
    //Using this context,we can create a DStream that represents streaming data from a textFile source
    val lines = ssc.textFileStream("/home/qingjian/spark_stream")
    
    //Count each word in each batch
    //Return a new DStream by applying a function to all elements of this DStream,
     //and then flattening the results
    val words = lines.flatMap(_.split(" "))
    
    val wordcount=words.map { x => (x,1) }.reduceByKey(_+_)
    wordcount.print()  //print a few of the counts generated every second
    ssc.start()   //start the computation
    ssc.awaitTermination() //wait for the computation to terminate
  }
}