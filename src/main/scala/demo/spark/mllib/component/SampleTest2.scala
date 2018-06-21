package demo.spark.mllib.component

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SampleTest2 {
  def main(args:Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(Array("aa","aaa","ddd","cc","dd","bb","bbb","ccc"), 2).map(d=>(d.length(),d))
    data.foreach(println)
    println("sample")
    val samp = data.sample(false, 0.5, 0)
    samp.foreach(println)
    
    println("sampleByKey")
    val fractions:Map[Int, Double] = Map(2->0.5, 3->0.8)
    val sampleByKeyRdd = data.sampleByKey(false, fractions, 0)
  
    sampleByKeyRdd.foreach(println)
    
  }
}