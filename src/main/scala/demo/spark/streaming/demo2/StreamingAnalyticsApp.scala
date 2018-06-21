package demo.spark.streaming.demo2

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import java.text.SimpleDateFormat
import java.util.Date

//需要先运行 StreamingProducer.scala
object StreamingAnalyticsApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("")
        val ssc = new StreamingContext(conf, Seconds(2))
        val stream = ssc.socketTextStream("localhost", 9999)
        
        val events = stream.map { record =>
            val event = record.split(",")
            //buyer   product   price  
            (event(0),event(1),event(2))
        }//MappedDStream
        
        /**
         * 计算并输出每一个批次的状态，因为每个批次都会生成RDD，所以在DStream上调用forEachRDD
         */
       
        events.foreachRDD{(rdd,time) =>
            val numPurchases = rdd.count()
            val uniqueUsers = rdd.map{case(user,_,_) => user}.distinct.count
            val totalRevenue = rdd.map{case(_,_,price)=>price.toDouble}.sum
            val productsByPopularity = rdd.map{ case(user,product,price)=>
                (product,1)
                
            }.reduceByKey(_+_)
            .collect
            .sortBy(-_._2)
            var mostPopular = ("",0)
            if(productsByPopularity.size>0)
                mostPopular = productsByPopularity(0)
                
            
            val formatter = new SimpleDateFormat
            val dateStr = formatter.format(new Date(time.milliseconds))
            println(s"== Batch start time: $dateStr ==")
            println("Total purchases: "+numPurchases)
            println("Unique users: "+uniqueUsers)
            println("Total revenue: "+totalRevenue)
            println("Most popular product: %s: with %d purchases".format(mostPopular._1, mostPopular._2))
            
        }
        
        ssc.start()
        ssc.awaitTermination()
      
    }
}