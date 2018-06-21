
package demo.spark.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

 
 //状态操作:相当于有一个全局变量进行统计
object StatefulWordlCount2 {
    //分好组的单词。Iterator[(key，values，以前该key的结果)]
    def updateFunc =  ( iter:Iterator[(String, Seq[Int], Option[Int])]) => {
         //iter.flatMap{ case(x,y,z)=>
         //    Some(y.sum+z.getOrElse(0)).map { m => (x, m) }
             
         //}
         //上面语句与下面语句结果一致
         //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map { x => (it._1, x) })
         //也与下面语句一样
         //iter.map(it=>(it._1, it._2.sum + it._3.getOrElse(0)))
         //也与下面语句一样
         iter.map{case(word_key, currentValues, previousResult)=>(word_key, currentValues.sum + previousResult.getOrElse(0))}
    }
    def main(args: Array[String]): Unit = {
        //屏蔽信息
        LoggerLevels.setStreamingLogLevels()
        
        val conf = new SparkConf().setAppName("StatefulWordlCount2").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))
        //updateStateByKey必须设置checkpoint 
        ssc.checkpoint(".")  //集群环境需要设置多节点共享存储（例如hdfs）。本地测试可以使用本地路径
        val data = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_ONLY)
        val stateDStream = data.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
        stateDStream.print()
      
        ssc.start()
        ssc.awaitTermination()
        
    }
  
}