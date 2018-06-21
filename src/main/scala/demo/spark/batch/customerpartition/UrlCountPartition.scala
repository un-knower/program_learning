package demo.spark.batch.customerpartition

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.net.URL
import org.apache.spark.Partitioner
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
/**
 * 实现 spark自定义分区
 * （1）使用自定义分区保证一个文件只保存一种key，即host
 * （2）每个文件中保存访问页面次数的前三个名，即分别求每个host相关的页面次数的前三名
 */
object UrlCountPartition {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("url count partition").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val dataRdd = sc.textFile("src/com/spark/demo/url.txt")
        val hostsUrlWithOneRdd = dataRdd.map { line => 
            val fields = line.split("\t")
            val url = fields(1)
            val urlObj = new URL(url) //提取host
            ((urlObj.getHost, url), 1)
        }
        val hostsUrlCountRdd = hostsUrlWithOneRdd.reduceByKey(_+_)
        //hostsCountRdd.saveAsTextFile("src/com/spark/demo/out") //此时会生成两个分区partition文件
        
        //hostsCountRdd.repartition(hostsCountRdd.count().toInt).saveAsTextFile("src/com/spark/demo/out") //重新分区HashPartition，此时生成4个文件，每个文件中按key.hashcode%numOfPartition进行分区。所以有可能一个分区文件中有多个key。这种现象叫做hash冲突
        //hostsCountRdd.coalesce(hostsCountRdd.count().toInt, false).saveAsTextFile("src/com/spark/demo/out") //2个分区文件，如果将false改为true，结果与repartition一样
        
        val hostAndUrlCountRdd = hostsUrlCountRdd.map{t=>
            (t._1._1,(t._1._2, t._2)) //key=host，value=(url,count)
        }
        
        val hostSet = hostAndUrlCountRdd.map(_._1).distinct.collect  //key即host的去重集合
        
        
        val hostPartitioner = new HostPartitioner(hostSet) //按照host进行分区
        hostAndUrlCountRdd.partitionBy(hostPartitioner)  //使用自定义分区，每个key根据规则进入不同的分区
                     .saveAsTextFile("src/com/spark/demo/out")
        //上一步保存了所有数据，保证了一个文件只有一种key
        //下面，将每个文件的的页面按访问次数排序，取top3             
        hostAndUrlCountRdd.partitionBy(hostPartitioner).mapPartitions(it=>{
            it.toList.sortBy(_._2._2).reverse.take(3).iterator //按照count进行递减排序，取top3，然后返回Iterator
        }).saveAsTextFile("src/com/spark/demo/out2")  //每个文件的的页面按访问次数排序，取top3  
        
        
        
        sc.stop()
    }
  
}


//自定义分区
class HostPartitioner(hostSet:Array[String]) extends Partitioner{
    val partMap = new HashMap[String,Int]() //定义规则
    var partitionId = 0 //分区号
    for(i <- hostSet) {
        partMap += (i->partitionId)
        partitionId += 1
    }
    def numPartitions: Int = { //定义多少个分区
        hostSet.length
    }

    def getPartition(key: Any): Int = {  //根据传入的key，返回分区号。这里是执行规则
       partMap.getOrElse(key.toString, 0)  
    }
}



