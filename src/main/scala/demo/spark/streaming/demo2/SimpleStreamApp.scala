package demo.spark.streaming.demo2

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

//需要先运行 StreamingProducer.scala
object SimpleStreamApp {
    def main(args: Array[String]): Unit = {
//        val conf = new SparkConf().setMaster("local[2]").setAppName("")
//        val ssc = new StreamingContext(conf, Seconds(2))
//        val stream = ssc.socketTextStream("localhost", 9999)
        val ssc = new StreamingContext("local[2]","app name",Seconds(10))
        val stream = ssc.socketTextStream("localhost", 9999)
        
        //简单的打印每一批的前几个元素
        stream.print()  //默认打印前10个
        ssc.start()
        ssc.awaitTermination()
      
    }
  
}