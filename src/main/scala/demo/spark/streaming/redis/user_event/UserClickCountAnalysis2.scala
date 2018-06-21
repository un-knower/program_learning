package demo.spark.streaming.redis.user_event

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.util.Properties
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import net.sf.json.JSONObject
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.{ Level, Logger }

object UserClickCountAnalysis2 {
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
    
    //Kafka Configuartion
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
    val kafkaStreaming: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val events = kafkaStreaming.flatMap(line => {
      val data = JSONObject.fromObject(line._2)
      Some(data)
    })
        
    //Compute user click times
    val userClicks = events.map(x => (x.getString("uid"), x.getInt("click_count"))).reduceByKey(_+_)
    userClicks.foreachRDD(rdd=>rdd.collect().foreach(println)) //输出打印
    
    userClicks.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        partitionOfRecords.foreach(pair => {
          /**
           * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
           */
          object InternalRedisClient extends Serializable {
            @transient private var pool:JedisPool = null
            
            def makePool(redisHost:String, redisPort:Int, redisTimeout:Int,maxTotal:Int, 
            				maxIdle:Int, minIdle:Int):Unit = {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }
            def makePool(redisHost:String, redisPort:Int, redisTimeout:Int,maxTotal:Int, 
                maxIdle:Int, minIdle:Int, testOnBorrow:Boolean, testOnReturn:Boolean, maxWaitMillis:Long):Unit = {
              if(pool == null ) {
                val poolConfig = new GenericObjectPoolConfig()
                poolConfig.setMaxTotal(maxTotal) //最大连接数, 默认8个
                poolConfig.setMaxIdle(maxIdle)  //最大空闲连接数, 默认8个
                poolConfig.setMinIdle(minIdle)  //最小空闲连接数, 默认0
                poolConfig.setTestOnBorrow(testOnBorrow)//在获取连接的时候检查有效性, 默认false；如果为true，则得到的jedis实例均是可用的；
                poolConfig.setTestOnReturn(testOnReturn) //在return给pool时，是否提前进行validate操作；
                poolConfig.setMaxWaitMillis(maxWaitMillis)//表示当borrow一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)   
                
                val hook = new Thread {
                  override def run = pool.destroy()
                  
                }
                sys.addShutdownHook(hook.run)
              }
            }
            
            def getPool:JedisPool = {
              assert(pool != null)
              pool
            }
            
          } //object InternalRedisClient
          
          
          //Redis Configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "SparkMaster"
          val redisPort  = 6379
          val redisTimeout = 30000
          val dbIndex = 1 //redis数据库为1
          InternalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
          
          val uid = pair._1
          val clickCount = pair._2
          val jedis = InternalRedisClient.getPool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy(clickHashKey, uid, clickCount)
          InternalRedisClient.getPool.returnResource(jedis)
          
          
        })
      })
    })
    
    ssc.start()
    ssc.awaitTermination()
    
  }
}