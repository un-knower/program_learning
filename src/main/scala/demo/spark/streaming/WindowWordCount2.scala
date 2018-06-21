

package demo.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object WindowWordCount2 {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint(".")
    
    //kafka参数  
    val kafkaParam = Map[String,String](   
      "metadata.broker.list"-> "sparkmaster:9092",  
      "group.id" -> "g1", //设置一下group id  
      "auto.offset.reset" -> kafka.api.OffsetRequest.LargestTimeString,  //从该topic最新的位置开始读数  
      "zookeeper.connection.timeout.ms" -> "10000"  
    )  
    val topics = "flinktest"
    val topicSet = topics.split(",").toSet
    
    //获取数据
    val directKafka: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParam,topicSet)
    val lines = directKafka.map(_._2)
    
    val words = lines.map { _.split(":")(0)}
    
    //window操作
    val wordCount = words.map { x => (x,1) }.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b), Seconds(10), Seconds(2))
   
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()  //waitForCompletion
  }
}