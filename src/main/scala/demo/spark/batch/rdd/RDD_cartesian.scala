package demo.spark.batch.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDD_cartesian {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("").setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    
    val data = sc.parallelize(List((3,'s'),(2,'t'),(6,'z'),(8,'c')))
    val data1 = sc.parallelize(List((1,'S'),(4,'T'),(7,'Z'),(9,'C')))
    
    val result = data.cartesian(data1)
    result.foreach(println)
    
    sc.stop()
    
  }
}