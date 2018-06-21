

package demo.spark.streaming.demo

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.log4j.Logger
import org.apache.log4j.Level
 ////nc -lk 9999
/**
 * 从socket中读数据
 */
object OutputSimulation {
  def main(args: Array[String]): Unit = {
     LoggerLevels.setStreamingLogLevels()
      
     val sparkConf = new SparkConf().setAppName("").setMaster("local[2]") //本地运行，2个线程，一个监听，一个处理数据
     val ssc = new StreamingContext(sparkConf,Seconds(1))
     //从socket端口中读数据
     val lines =ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY) //还有个只有前两参数的构造方法，第三个参数默认是storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
     //lines是Dstream类型的 
     val words = lines.flatMap {_.split(" ") }
     val wordcount = words.map { x => (x,1) }.reduceByKey(_+_)
     wordcount.print()
     ssc.start() //sparkstreaming需要启动任务，处于监听状态
     ssc.awaitTermination()
    
     
  }
}