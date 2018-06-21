package demo.spark.streaming.demo2

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
 * 有状态的流计算
 * 使用updateStateByKey函数基于状态流计算营收和所有用户购买量这个全局状态
 */

//需要先运行 StreamingProducer.scala
object StreamingStateApp {  //             产品名         价格                                                                             所有用户购买量  价格总额                                          
    def updateState(productAndPrice:Seq[(String, Double)], currentTotal:Option[(Int, Double)]) = { //第一个参数是本次进来的值，第二个是过去处理后保存的值
        val currentRevenue = productAndPrice.map(_._2).sum
        val currentNumberPurchases = productAndPrice.size
        val state = currentTotal.getOrElse(0, 0.0)
        Some(
                (currentNumberPurchases+ state._1, currentRevenue+state._2)
             )
                        
        
    }
    def main(args: Array[String]): Unit = {
        val ssc = new StreamingContext("local[2]","state streaming", Seconds(2))
        //对于有状态的操作，需要设置一个检查点
        ssc.checkpoint("./sparkstreaming")
        val stream = ssc.socketTextStream("localhost", 9999)
        
        //基于原始文本元素生成活动流
        val events = stream.map { record =>  
            val event = record.split(",")    
            //buyer   product   price
            (event(0),event(1),event(2).toDouble)
        
        }
        
        val users = events.map{ case(user,product,price)=>
            (user,(product,price))
        }
        
        val revenuePerUser  = users.updateStateByKey(updateState) //按人进行分组关联
        revenuePerUser.print()  //将全局结果打印出来
        
        //
        ssc.start()
        ssc.awaitTermination()
      
    }
  
}