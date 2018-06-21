package demo.spark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object GetMax {
  def getMax(a:Int, b:Int) = {
      Math.max(a, b)
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(Seq(1,3,6,4,20,10,7))
    
    val result = data.reduce(getMax)
    println(result)
    
  }
}