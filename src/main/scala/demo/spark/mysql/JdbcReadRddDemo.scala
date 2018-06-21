package demo.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD
/**
 * 读取mysql数据库，将数据转换成jdbc。
 * 这种方法有局限性，partitionNum需要设置为1，且sql有局限性
 * 
 * 不如ReadFromMysql.scala：将数据转成DataFrame
 */
object JdbcReadRddDemo {
  def myfunc7[T](index:Int, iter:Iterator[(T,T,T,T)]):Iterator[String] = {
                iter.toList.map(x => "[partID:" +  index + ", val: " + x + "]").iterator
    }
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("jdbc rdd").setMaster("local[2]")
        val sc = new SparkContext(conf)
        
        def getConnection() = {
        //下句与上句一样
        //val getConnection = ()=>{    
            Class.forName("com.mysql.jdbc.Driver").newInstance()
            DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root")
        }
        
        val jdbcRdd = new JdbcRDD(
                sc,  //sc
                getConnection,  //Connection
                "select * from location_info where id > ? and id < ?", //sql 有局限性
                1,7,
                1, //partitonNum必须为1，大于1会导致数据读取不全！
                rs => { //ResultSet =>
                    val id = rs.getInt(1)
                    val countryName = rs.getString(2)
                    val count = rs.getString(3)
                    val create_time = rs.getDate(4)
                    (id, countryName, count, create_time)
                }
             )
        
        println(jdbcRdd.partitions.size)
        jdbcRdd.mapPartitionsWithIndex(myfunc7).collect.foreach(println)
        println(jdbcRdd.collect.toBuffer)
        
        sc.stop()
    }
}