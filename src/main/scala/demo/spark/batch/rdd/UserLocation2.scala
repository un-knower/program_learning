package demo.spark.batch.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 求用户停留的时间最长的两个地点
 */
object UserLocation2 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("userlocation2").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val data = sc.textFile("src/com/spark/demo/log")
        val siteToCoordinate = sc.textFile("src/com/spark/demo/loc_info.txt")
        val coordinatRdd = siteToCoordinate.map { line => 
            val f = line.split(",")
            //（信号塔id， （经度，纬度））
            (f(0), (f(1),f(2)) )
        }
        
        
        val rdd0 = data.map{ d=>
            val arr = d.split(",")
            val eventType = arr(3)
            val time = arr(1) //时间
            val timeLong = if(eventType == "1"){ //如果时间类型是1建立链接
                -time.toLong
            }else {
                +time.toLong
            }
                
            ((arr(0),arr(2)), timeLong) //key是一个元组。(key=(电话号码,信号塔id)，  value=timeLong)
        }
        
        val rdd1 = rdd0.reduceByKey(_+_).map(t=>{
            val mobile = t._1._1
            val site = t._1._2
            val stayTime = t._2
            (site,(mobile,stayTime))
        })
        val rdd2 = rdd1.join(coordinatRdd).map{t=>
            val site = t._1    
            val mobile = t._2._1._1
            val stayTime = t._2._1._2
            val x = t._2._2._1
            val y = t._2._2._2
            (mobile, site, stayTime, x, y)
        }
        val rdd3 = rdd2.groupBy(_._1).mapValues(it=>{
          it.toList.sortBy(_._3).reverse.take(2)
        })
        
        rdd3.saveAsTextFile("src/com/spark/demo/out")
        
        
        
        println(rdd3.collect().toBuffer)
       sc.stop() 
        
    }
  
}