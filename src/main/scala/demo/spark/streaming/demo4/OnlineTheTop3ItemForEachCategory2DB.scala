package demo.spark.streaming.demo4

import demo.spark.streaming.mysql.ConnectionPool
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType


/**

  * 使用Spark Streaming+Spark SQL来在线动态计算电商中不同类别中最热门的商品排名，例如手机这个类别下面最热门的三种手机、电视这个类别

  * 下最热门的三种电视，该实例在实际生产环境下具有非常重大的意义；

  * 实现技术：Spark Streaming+Spark SQL，之所以Spark Streaming能够使用ML、sql、graphx等功能是因为有foreachRDD和Transform

  * 等接口，这些接口中其实是基于RDD进行操作，所以以RDD为基石，就可以直接使用Spark其它所有的功能，就像直接调用API一样简单。

  * 数据的格式：user item category，例如Rocky Samsung Android

  */
object OnlineTheTop3ItemForEachCategory2DB {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("").setMaster("spark://SparkMaster:7077")
         //设置batchDuration时间间隔来控制Job生成的频率并且创建Spark Streaming执行的入口
        val ssc = new StreamingContext(conf, Seconds(5))
        ssc.checkpoint("/data/streaming_checkpoint")
        val userClickLogsDStream = ssc.socketTextStream("SparkMaster", 9999)
        val hiveContext = new HiveContext(ssc.sparkContext)
        val structType = StructType(Array( //创建hive表用的结构
            //             字段名                    字段类型       是否可以为空
            StructField("category",StringType,true),      
            StructField("item",StringType,true),     
            StructField("click_count",IntegerType,true)      
        ))
        val formuserClickLogs = userClickLogsDStream.map { clickLog =>  
            val split = clickLog.split("\t")
            (split(2)+"_"+split(1),1) // (`category`_`item`,1 ) 点击次数
        }
        
        val categoryUserClickLogsDSteam = formuserClickLogs.reduceByKeyAndWindow(_+_, _-_, Seconds(60), Seconds(20))
        
        categoryUserClickLogsDSteam.foreachRDD{rdd=>
            if(rdd.isEmpty()){ //判断RDD是否为空
                println("No data input！")
            }
            
            else {
                val categoryItemRow = rdd.map(reduceItem=>{
                    val category = reduceItem._1.split("_")(0)
                    val item = reduceItem._1.split("_")(1)
                    val clickcount = reduceItem._2    
                    Row(category, item, clickcount) 
                    
                })
                
                ////生产环境下注意用hiveContext，其继承了SparkContext所有功能
                val categoryItemDF = hiveContext.createDataFrame(categoryItemRow, structType)          
                categoryItemDF.registerTempTable("categoryItemTable")
                
                val reseltDataFram = hiveContext.sql("SELECT category,item,click_count FROM (SELECT category,item,click_count,row_number()" +
                                           " OVER (PARTITION BY category ORDER BY click_count DESC) rank FROM categoryItemTable) subquery " +
                                           " WHERE rank <= 3")
                reseltDataFram.show()
                //得到结果
                val resultRowRdd = reseltDataFram.rdd
                                           
                resultRowRdd.foreachPartition { partitionOfRecords => //使用partiton为入库mysql做准备 
                    if(partitionOfRecords.isEmpty) {
                        println("This Rdd is not null but partiton is null")
                        
                    }else {
                        val connection = ConnectionPool.getConnection
                        partitionOfRecords.foreach {record=>
                            val sql = "insert into categorytop3(category,item,client_count) values('" + record.getAs("category") + "','" +
                                    record.getAs("item") + "'," + record.getAs("click_count") + ")"
                            
                            val stmt = connection.createStatement()
                            stmt.executeUpdate(sql)
                        }  
                    }
                
                } //resultRowRdd                          
                                           
                
            }
            
        }//categoryUserClickLogsDSteam
        
        ssc.start
        ssc.awaitTermination()
    
    }
  
}