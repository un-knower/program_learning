package demo.spark.batch.broadcast

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.DriverManager
 

/**
 * 根据ip地址查找其所属
 * 将ip与所属地址的规则broadcast广播到各个executor中
 * 
 * 测试数据库表
 * insert into test.location_info(location,num) values("test",22);
 * 
 * 
 * 
 test.location_info表结构：
 CREATE TABLE `location_info` (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT '自增长id',
  `location` varchar(20) DEFAULT NULL COMMENT '地区（国家名）',
  `num` int(10) DEFAULT NULL COMMENT '访问次数',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
 */
object IpLocation {
    //将rdd中的数据持久化到mysql中，插入字段为(String, Int)
    val data2MySql = (iterator:Iterator[(String, Int)]) => {
        var conn:Connection = null
        var ps:PreparedStatement = null
        val sql = "insert into test.location_info(location, num) values (?, ?)"
        try {
            
          conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
          iterator.foreach(line => {
              ps = conn.prepareStatement(sql)
              ps.setString(1, line._1)    
              ps.setInt(2, line._2)
              ps.executeUpdate()
          })
            
        } catch {
          case t: Exception => {
                  println("MySQL Exception!")
                  t.printStackTrace()// TODO: handle error
          } 
        } finally {
            if(ps != null) {
                ps.close()
            }
            if(conn != null) {
                conn.close()
            }
        }
        //iterator mapPartitions需要这个返回值
    }
            
    
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("ip location").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        val ipRulesRdd = sc.textFile("src\\com\\spark\\demo\\broadcast\\ip-by-country.csv").map { line => 
            val fields = line.split(",")
            val startIpNum = fields(2)
            val endIpNum = fields(3)
            val cityName = fields(5)
            (startIpNum, endIpNum, cityName)
        }
        //全部的ip映射规则
        val ipRulesArray = ipRulesRdd.collect //触发action，将数据收集到driver，为后面广播做准备。该变量值在driver中有，worker中不存在
        val bIpRules = sc.broadcast(ipRulesArray)
        
        //加载要处理的数据
        val ipsRdd = sc.textFile("src\\com\\spark\\demo\\broadcast\\access.log").map { line =>  
            val fields = line.split("\t")
            val ip = fields(1)
            ip
        }
        
        val result = ipsRdd.map { ip => 
            val ipNum = IpUtil.ip2Long(ip)
            val info = IpUtil.searchContentByKey(bIpRules.value, ipNum)
            val countryName = info.split(",")(2)
            (ip, countryName)
        }
        
        result.collect().foreach(println)
        
        //进行统计
        val calcRdd = result.map(t=>(t._2, 1)).reduceByKey(_+_)
        //将统计信息持久化到mysql
        //calcRdd.mapPartitions(data2MySql)这么写不太好，因为会产生一个新的rdd，造成浪费
        calcRdd.foreachPartition(data2MySql)
        
        
        sc.stop()
    }
  
}