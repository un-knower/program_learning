package demo.spark.mllib.component

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector,Vectors}
import org.apache.spark.sql.SQLContext

object MinMaxScalerTest {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("").setMaster("local[4]")
      val sc = new SparkContext(conf)
      val ssc = new SparkContext(conf)
      val observations = sc.parallelize(Seq(
        Vectors.dense(1.0,10.0,100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
    ))
    
    val sqlContext = new SQLContext(sc) 
    import sqlContext.implicits._ 
    
    val filePath = args(0)
    val userInfo = sc.textFile(filePath).toDF
    
  }
}