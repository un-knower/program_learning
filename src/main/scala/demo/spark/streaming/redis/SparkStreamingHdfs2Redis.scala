package demo.spark.streaming.redis

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import redis.clients.jedis.Jedis
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Durations
import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

object SparkStreamingHdfs2Redis {
  def main(args: Array[String]) {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //Create a StreamingContext
    var masterUrl = "local[4]"
    if (args.length > 0) {
      masterUrl = args(0)
    }
    
    val conf = new SparkConf().setAppName("SparkStreamingHdfs2Redis").setMaster(masterUrl)
    val ssc = new StreamingContext(conf, Durations.seconds(5))
    val line = ssc.textFileStream("hdfs://sparkmaster:9000/eclipse/monitorhdfs") //只能监控当前一层目录
    val result = line.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _)
    val wordCountHashKey = "word:count"
    result.foreachRDD(rdd => {
      rdd.foreachPartition(parti => {
        /**
           * Internal Redis client for managing Redis connection {@link Jedis} based on {@link RedisPool}
           */
          object InnernalRedisClient extends Serializable {
            @transient private var pool: JedisPool = null
            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int,
                         maxIdle: Int, minIdle: Int) {
              makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle, true, false, 10000)
            }
            def makePool(redisHost: String, redisPort: Int, redisTimeout: Int, maxTotal: Int,
                         maxIdle: Int, minIdle: Int, testOnBorrow: Boolean, testOnReturn: Boolean, maxWaitMillis: Long): Unit = {
              if (pool == null) {
                val poolConfig = new GenericObjectPoolConfig()
                poolConfig.setMaxTotal(maxTotal)
                poolConfig.setMaxIdle(maxIdle)
                poolConfig.setMinIdle(minIdle)
                poolConfig.setTestOnBorrow(testOnBorrow)
                poolConfig.setTestOnReturn(testOnReturn)
                poolConfig.setMaxWaitMillis(maxWaitMillis)

                pool = new JedisPool(poolConfig, redisHost, redisPort, redisTimeout)
                
                val hook = new Thread {
                  override def run = pool.destroy()
                }
                sys.addShutdownHook(hook.run()) //当程序关闭时，销毁线程池
              }

            }//def makePool

            def getPool:JedisPool = {
              assert(pool != null)
              pool
            }
          } //object InnernalRedisClient
          
          
          //Redis Configurations
          val maxTotal = 10
          val maxIdle = 10
          val minIdle = 1
          val redisHost = "SparkMaster"
          val redisPort = 6379
          val redisTimeout = 30000
          val dbIndex = 1
          InnernalRedisClient.makePool(redisHost, redisPort, redisTimeout, maxTotal, maxIdle, minIdle)
        parti.foreach(pair => {
          val word = pair._1
          val count = pair._2
          val jedis = InnernalRedisClient.getPool.getResource
          jedis.select(dbIndex)
          jedis.hincrBy(wordCountHashKey, word, count)
          InnernalRedisClient.getPool.returnResource(jedis) //回收资源
        })
      })
    })
    
    ssc.start()
    ssc.awaitTermination()

  }
}