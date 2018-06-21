

package demo.spark.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint(".")
    
    //获取数据
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap { _.split(" ")}
    
    //window操作
    val wordCount = words.map { x => (x,1) }.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b), Seconds(30), Seconds(10))
   
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()  //waitForCompletion
    StreamingContext
  }
}