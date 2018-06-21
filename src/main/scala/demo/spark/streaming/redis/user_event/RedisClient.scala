package demo.spark.streaming.redis.user_event

import redis.clients.jedis.JedisPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig

object RedisClient {
  val redisHost = "sparkmaster"
  val redisPort = 6379
  val redisTimeOut = 30000
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeOut)
  
  lazy val hook = new Thread {
    override def run = {
      print("Execute hook thread:"+this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}