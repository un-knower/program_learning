package demo.spark.streaming.demo4

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext

//import org.apache.spark.sql.hive.HiveContext

object TestOnlineTheTop3ItemForEachCategory2DB {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("").setMaster("local[4]") 
        val sc = new SparkContext(conf )
        
        val stream = sc.textFile("src/com/sparkstreaming/demo4/file.txt")
        
        
        val streamTrans = stream.map{ line =>
            
            val split = line.split(",")
       		val user = split(0)
       		val item = split(1)
       		val category = split(2)
       		val price = split(3)
            ((item,category),(user,price.toDouble))
        }
        
        //统计一共有多少个分类
        val categoryNum = streamTrans.map(_._1._1).distinct().count()
        println(s"distinct category num = $categoryNum")
        
        //统计每个分类中的每个物品的总价格
        val streamTransGroup = streamTrans.groupByKey()
        streamTransGroup.foreach(println)
        streamTransGroup.foreach{pair=>
            val item = pair._1._1
            val category = pair._1._2
            val lis = pair._2.toList
            val sum = lis.map(_._2).sum
            
            println(s"$category:$item money sum=$sum")
        }
     
    }
  
}