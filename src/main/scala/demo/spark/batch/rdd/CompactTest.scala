package demo.spark.batch.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

object CompactTest {
  def main(args:Array[String]) {

    val s = List("123", "234", "345", "456", "567", "678")
    val n = 2
    val conf = new SparkConf().setAppName("").setMaster("local")
    val sc = new SparkContext(conf)
    
    val rdd = sc.parallelize(s, n).mapPartitions(iter  => {
        val result = ArrayBuffer[String]()
        val tmp = ArrayBuffer[String]()
        
        while(iter.hasNext) {
           tmp += (iter.next())
           
        }
        result.+:(tmp.mkString(" ")).iterator
         
      }
    )
    rdd.collect().foreach(println)
  }
}