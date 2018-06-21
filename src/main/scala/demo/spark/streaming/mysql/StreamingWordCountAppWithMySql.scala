package demo.spark.streaming.mysql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
 * 基于mysql保存中间结果，进行数量累计
 * 
 MySql 表：
  
 CREATE TABLE `streaming_world_count` (
  `word` varchar(20) DEFAULT NULL,
  `count` int(11) DEFAULT NULL
 ) ENGINE=MyISAM DEFAULT CHARSET=utf8
 
 
 
 */
object StreamingWordCountAppWithMySql {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[5]").setAppName("StreamingWordCountAppWithMySql")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))
        
        val stream = ssc.socketTextStream("localhost", 9999)
        stream.print() //打印流信息
        val wordCount = stream.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)  
        wordCount.foreachRDD{rdd=>
            rdd.foreachPartition{partitionOfRecords=> {
                //ConnectionPool is a static,lazily initialized pool of connections    
                val connection = ConnectionPool.getConnection()
                partitionOfRecords.foreach(record=> {
                    val sql = "insert into streaming_world_count(word,count) values('"+record._1+"', "+record._2+")"
                    val stmt = connection.createStatement()
                    stmt.executeUpdate(sql);
                })
                ConnectionPool.returnConnection(connection)
            }
          }
            
        }
                
        ssc.start()
        ssc.awaitTermination()
        
    }
  
}