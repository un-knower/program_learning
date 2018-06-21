package demo.spark.mysql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.rdd.RDD
/**
 * 读mysql，将数据转换成DataFrame，之后就是DataFrame的操作了
 * 通过创建SQLContext的单例对象，来read数据库

CREATE TABLE `location_info` (
  `id` int(20) NOT NULL AUTO_INCREMENT COMMENT '自增长id',
  `location` varchar(20) DEFAULT NULL COMMENT '地区（国家名）',
  `num` int(10) DEFAULT NULL COMMENT '访问次数',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=15 DEFAULT CHARSET=utf8;

 
 */
object ReadFromMysql {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("jdbc demo")
        val sc = new SparkContext(conf)
        val sqlContext = SQLContextSingleton.getInstance(sc)
        //import sqlContext.implicits._
        
        val jdbcDataFrame = sqlContext.read.format("jdbc").options(
                Map(
                        "url"->"jdbc:mysql://localhost:3306/test",
                        "driver"->"com.mysql.jdbc.Driver",
                        "dbtable"->"location_info",
                        "user"->"root",
                        "password"->"root"
                )
                
        ).load() //返回DataFrame
        //打印全部
        jdbcDataFrame.collect().foreach(println)
        
        //打印top20
        jdbcDataFrame.show()
      
        /*DataFrame操作*/
        
        //打印schema
        jdbcDataFrame.printSchema()
//root
// |-- id: integer (nullable = false)
// |-- location: string (nullable = true)
// |-- num: integer (nullable = true)
// |-- create_time: timestamp (nullable = true)        
        
        //打印id
        //select("","")
        jdbcDataFrame.select("id").show
//+--+
//|id|
//+--+
//| 1|
//| 2|
//| 3|
//| 4|
//| 5|
//| 6|
//| 7|
//| 8|
//| 9|
//|10|
//|11|
//|12|
//|13|
//|14|
//+--+        
        //打印id,location
        jdbcDataFrame.select("id","location").show
//+--+------------------+
//|id|          location|
//+--+------------------+
//| 1|              test|
//| 2|           Bolivia|
//| 3|             China|
//| 4|         Sri Lanka|
//| 5|            Brazil|
//| 6|           Germany|
//| 7|            Canada|
//| 8|             Japan|
//| 9|     United States|
//|10|             ERROR|
//|11|       Netherlands|
//|12|             Korea|
//|13|Russian Federation|
//|14|           Denmark|
//+--+------------------+
        
        
        //select(colume1, colume2+10)
        val locationColume = jdbcDataFrame("location")
        val numColume = jdbcDataFrame("num")
        jdbcDataFrame.select(locationColume, numColume+10).show
//+------------------+----------+
//|          location|(num + 10)|
//+------------------+----------+
//|              test|        32|
//|           Bolivia|        11|
//|             China|        75|
//|         Sri Lanka|        11|
//|            Brazil|        13|
//|           Germany|        12|
//|            Canada|        11|
//|             Japan|        12|
//|     United States|        58|
//|             ERROR|        49|
//|       Netherlands|        13|
//|             Korea|        13|
//|Russian Federation|        11|
//|           Denmark|        11|
//+------------------+----------+     
        
        //筛选 filter
        jdbcDataFrame.filter(jdbcDataFrame("id") >= 3 && jdbcDataFrame("id") <= 10).show
//+--+-------------+---+--------------------+
//|id|     location|num|         create_time|
//+--+-------------+---+--------------------+
//| 3|        China| 65|2016-12-02 17:47:...|
//| 4|    Sri Lanka|  1|2016-12-02 17:47:...|
//| 5|       Brazil|  3|2016-12-02 17:47:...|
//| 6|      Germany|  2|2016-12-02 17:47:...|
//| 7|       Canada|  1|2016-12-02 17:47:...|
//| 8|        Japan|  2|2016-12-02 17:47:...|
//| 9|United States| 48|2016-12-02 17:47:...|
//|10|        ERROR| 39|2016-12-02 17:47:...|
//+--+-------------+---+--------------------+        
        
        //groupBy
        jdbcDataFrame.groupBy("num").count.show
//+---+-----+
//|num|count|
//+---+-----+
//| 39|    1|
//| 48|    1|
//| 65|    1|
//|  1|    5|
//|  2|    2|
//|  3|    3|
//| 22|    1|
//+---+-----+
        import sqlContext.implicits._   //需要加上这句，否则下句报错！！
        jdbcDataFrame.groupBy("num").count.filter($"count"===1).show
//+---+-----+
//|num|count|
//+---+-----+
//| 39|    1|
//| 48|    1|
//| 65|    1|
//| 22|    1|
//+---+-----+        
        
        //map结构的Rdd
        val locaction2NumRdd : Dataset[Map[String, Any]] = jdbcDataFrame.map(_.getValuesMap[Any](List("location","num")))

        //DataFrame 保存
        //jdbcDataFrame.write.format("parquet").save("location_info.parquet")

    }
  
}

object SQLContextSingleton {

  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}