package demo.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD
import java.text.SimpleDateFormat
import java.sql.Connection
import java.sql.PreparedStatement

case class PersonInfo(no:Int, name:String, birthday:java.sql.Date)
object JdbcWriteRddDemo {
    def data2MySql(iterator:Iterator[PersonInfo]):Unit= {
         var conn:Connection = null
         var ps:PreparedStatement = null
         var sql = "insert into mysql_rdd(no,name,birthday) values(?,?,?)"
         try {
             val url = "jdbc:mysql://localhost:3306/test"
             conn = DriverManager.getConnection(url, "root", "root")
             iterator.foreach {info=>
                 ps = conn.prepareStatement(sql)
                 ps.setInt(1, info.no)
                 ps.setString(2, info.name)
                 ps.setDate(3, info.birthday)
                 ps.executeUpdate()
             }
         }catch {
           case t: Throwable => t.printStackTrace() // TODO: handle error
         }finally {
            if(ps != null) {
                ps.close()
            }
            if(conn != null) {
                conn.close()
            }
        }
         
         
    }
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("jdbc rdd").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val format =  new SimpleDateFormat("yyyy-MM-dd");
        //通过并行化创建RDD
        val personRdd = sc.parallelize(
           Array(
                   "1 tom 2016-01-05",
                   "2 jerry 2016-11-30",
                   "3 scott 2016-06-01",
                   "4 xyz 2016-12-10",
                   "5 abc 2016-02-16"
           )
                
        ).map{f=>
          val arr = f.split(" ")
          PersonInfo(arr(0).toInt ,arr(1), new java.sql.Date(format.parse(arr(2)).getTime))      
        }
        
        personRdd.foreachPartition(data2MySql)
        
        
    }
  
}