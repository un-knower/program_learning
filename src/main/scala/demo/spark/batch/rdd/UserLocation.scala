package demo.spark.batch.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * 求用户停留的时间最长的两个地点
 */
//用户定位信息（电话号码， 时间【年月日时分秒】， 信号塔id， 出入signal【1表示进入链接，0表示出去断开】）
//case class UserLocationInfo(phone:String,time:Long,site:String,signal:Int) 
object UserLocation {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("userlocation").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val data = sc.textFile("src/com/spark/demo/log")
        //data.foreach { println }
        val rdd1 = data.map{ d=>
            val arr = d.split(",")
            val eventType = arr(3)
            val time = arr(1) //时间
            val timeLong = if(eventType == "1"){ //如果时间类型是1建立链接
                -time.toLong
            }else {
                +time.toLong
            }
                
            (arr(0)+"_"+arr(2), timeLong) //(key=电话号码_信号塔id，  value=timeLong)
        } 
        
        //println(rdd1.collect().toBuffer)
        
        //根据key进行汇总
        //groupBy方法不会，会产生大量的数据Shuffle                                                  ( 手机号_信号塔id，停留时间)
        val rdd2 = rdd1.groupBy(_._1).mapValues(_.foldLeft(0L)(_ + _._2)) //ArrayBuffer((18611132889_9F36407EAD0629FC166F14DDE7970F68,154000),....)
        //按照手机号进行分组        
                
        val rdd3 = rdd2.map{t=>
            val mobileAndSite = t._1.split("_")
            val mobile = mobileAndSite(0)
            val site = mobileAndSite(1)
            val stayTime = t._2
            (mobile,site,stayTime)
        }
        
        val rdd4 = rdd3.groupBy(_._1).mapValues(it=>{
          it.toList.sortBy(_._3).reverse.take(2)  
        
        })
//ArrayBuffer(
//(18688888888,List((18688888888,16030401EAFB68F1E3CDF819735E1C66,87600), (18688888888,9F36407EAD0629FC166F14DDE7970F68,51200))), 
//(18611132889,List((18611132889,9F36407EAD0629FC166F14DDE7970F68,154000), (18611132889,A3030V01BAFB6851U6CDO8197O5E1CV9,110000))), 
//(13059155871,List((13059155871,A3030V01BAFB6851U6CDO8197O5E1CV9,138000)))
//)
        println(rdd4.collect().toBuffer)
        
        
        
        
        /**
         * 较好的方法是先在局部进行聚合，然后在最终汇总
         * 见UserLocation2
         * **/
        
        
        
        
        sc.stop
    }
}