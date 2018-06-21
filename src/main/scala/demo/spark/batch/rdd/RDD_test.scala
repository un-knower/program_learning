package demo.spark.batch.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object RDD_test {
  def main(args:Array[String]) {
      val conf = new SparkConf().setAppName("").setMaster("local[2]")
      val sc = new SparkContext(conf)
      
      val rdd1 = sc.parallelize(Array((1,1),(1,2),(2,1),(3,1)),2)
      val rdd2 = sc.parallelize(Array((1,'x'),(2,'y'),(2,'z'),(4,'w')),1)
      val substractByKeyRdd = rdd1.subtractByKey(rdd2);
      println("-"*10)
      substractByKeyRdd.collect().foreach(println)     
      
      val rdd3 = sc.parallelize(Array((1,1),(1,2),(3,3),(2,4),(2,1),(3,1)),2)
      val rdd = rdd3.groupByKey.map(x=>(x._1,x._2.last))
      rdd.collect.foreach(println)
      
      println("-"*10)
      println(rdd1.collect.head)
      println(rdd1.collect().tail)
      println(rdd1.collect().init)
      println(rdd1.collect().last)
      
      
  }
  
  
}