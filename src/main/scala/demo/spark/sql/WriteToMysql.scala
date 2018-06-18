package demo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import java.util.Properties
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DataTypes

/**
 * 将数据写入mysql
CREATE TABLE `mysql_rdd` (
  `id` int(20) NOT NULL AUTO_INCREMENT,
  `no` int(10) DEFAULT NULL,
  `name` varchar(20) DEFAULT NULL,
  `birthday` date DEFAULT NULL,
  `create_time` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8; 
 
 */
object WriteToMysql {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("jdbc demo")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)
        //通过并行化创建RDD
        val personRdd = sc.parallelize(
           Array(
                   "1 tom 2016-01-05",
                   "2 jerry 2016-11-30",
                   "3 scott 2016-06-01",
                   "4 xyz 2016-12-10",
                   "5 abc 2016-02-16"
           )
                
        ).map(_.split(" "))
                
        //通过StructType直接指定每个字段的schema
        val schema = StructType(
            List(
                // 字段名，字段类型，是否允许为空
                //StructField("id", IntegerType, true),        
                StructField("no", DataTypes.IntegerType, true),        
                StructField("name", StringType, true),       
                StructField("birthday", DateType, true)        
                //StructField("create_time", DateType, true)        
            )        
        )
        
        //将Rdd转成RowRdd
        val format =  new SimpleDateFormat("yyyy-MM-dd");
        val rowRdd = personRdd.map { p => Row(p(0).toInt,p(1), new java.sql.Date(format.parse(p(2)).getTime)) }
        //将schema信息应用到rowRdd
        val personDataFrame = sqlContext.createDataFrame(rowRdd, schema)
        //创建Properties存储数据库相关属性
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "root")
        //将数据追加到数据库
        val url = "jdbc:mysql://localhost:3306/test"
        val table = "mysql_rdd"
        personDataFrame.write.mode(SaveMode.Append).jdbc(url, table, prop)
        
        //
        sc.stop
        
    }
  
}