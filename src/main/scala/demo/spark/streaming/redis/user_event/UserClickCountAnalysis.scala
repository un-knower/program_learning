package demo.spark.streaming.redis.user_event

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.log4j.{ Level, Logger }
/**
 * 
 * 需要 RedisClient.scala
 * 
 * 
 * 在Spark集群环境部署Application后，在进行计算的时候会将作用于RDD数据集上的函数（Functions）发送到集群中Worker上的Executor上
 * （在Spark Streaming中是作用于DStream的操作），
 * 那么这些函数操作所作用的对象（Elements）必须是可序列化的，
 * 通过Scala也可以使用lazy引用来解决，否则这些对象（Elements）在跨节点序列化传输后，无法正确地执行反序列化重构成实际可用的对象。
 * 代码我们使用lazy引用（Lazy Reference）来实现的，代码如下所示：
 * 
 * local[K]和Spark Standalone集群模式下运行通过
 * 但是在Spark Standalone、YARN Client和YARN Cluster或Mesos集群模式部署的时候，就会报错
 * 主要是由于在处理Redis连接池或连接的时候出错，所以使用UserClickCountAnalysis2.scala
 * 
 */
object UserClickCountAnalysis {
  def main(args:Array[String]) {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    
    //Create a StreamingContext
    var masterUrl = "local[4]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    val conf = new SparkConf().setAppName("UserClickCountStat").setMaster(masterUrl)
    val ssc = new StreamingContext(conf, Seconds(5))
    
    //Kafka configuration
    val topics = Set("user_events") //kafka topic
    val brokers = "SparkMaster:9092"
    val kafkaParams = Map[String, String](
        "metadata.broker.list" -> brokers, 
        "zookeeper.connect" -> "SparkMaster:2181", 
        "group.id" -> "group1", 
        "serializer.class" -> "kafka.serializer.StringEncoder")
    
    val dbIndex = 1  //jedis的数据库选择1
    val clickHashKey = "app::users::click"
    
    //kafka中的内容：{"uid":"d7f141563005d1b5d0d3dd30138f3f62","event_time":"1506052901319","os_type":"Android","click_count":4}
    //Create a direct stream                                                          key    value      key的编码           value的编码
    val kafkaStream : InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics);
    val events = kafkaStream.flatMap(line => {
      val data = JSONObject.fromObject(line._2) //JSONObject  {"uid":"d7f141563005d1b5d0d3dd30138f3f62","event_time":"1506052901319","os_type":"Android","click_count":4}
      Some(data)
    })
    
    //Compute user click times
    val userClick = events.map(x=> (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_+_)
    userClick.foreachRDD(rdd=>rdd.collect().foreach(println)) //输出打印
    userClick.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          val uid = pair._1
          val clickCount = pair._2
          val jedis = RedisClient.pool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy(clickHashKey, uid, clickCount)
          RedisClient.pool.returnResource(jedis)
          
        })
      })
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}