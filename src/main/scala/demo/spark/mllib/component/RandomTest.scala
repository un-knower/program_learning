package demo.spark.mllib.component

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs._
object RandomTest {
  def main(args:Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("")
    val sc = new SparkContext(conf)
    val randomNumRdd = normalRDD(sc, 100)
    
    randomNumRdd.foreach(println)
  
  }
}