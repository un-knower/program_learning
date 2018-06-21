package demo.spark.batch.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDD_Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val a = sc.parallelize(1 to 9, 3)
//    val b = a.map { x => x*2 }
//    println(a.collect())
//    println(b.collect())
   lazy val EPSILON = {
    var eps = 1.0
    while ((1.0 + (eps / 2.0)) != 1.0) {
      eps /= 2.0
    }
    eps
  }
    println(EPSILON)//2.220446049250313E-16
    
  }
}