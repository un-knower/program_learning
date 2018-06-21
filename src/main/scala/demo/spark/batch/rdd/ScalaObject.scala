package demo.spark.batch.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ambitous on 2017/8/28.
  */
object ScalaObject {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("spark://sparkmaster:7077").setAppName("wordcount")
    val sc = new SparkContext(conf)
    val input = sc.parallelize(Array("hello wolrd","hello spark","hello !"))
    input.foreach(println)
//    val input = sc.textFile("/home/hadoop/hellospark.txt")
//    val lines = input.flatMap(_.split(" "))
//    val count = lines.map((_,1)).reduceByKey(_+_)
//    val output = count.foreach(println)
//    val out = count.saveAsTextFile("/home/hadoop/out")
  }

  def factorialTailrec(n: BigInt, acc: BigInt): BigInt = {
    if(n <= 1) acc
    else factorialTailrec(n-1, acc * n)
  }

  def sum(f: Int => Int)(a: Int)(b: Int): Int = {

    @annotation.tailrec
    def loop(n: Int, acc: Int): Int = {
      if(n>b){
        println(s"n=${n}, acc=${acc}")
        acc
      }else{
        println(s"n=${n}, acc=${acc}")
        loop(n+1, acc+f(n))
      }
    }
    loop(a, 0)
  }

}


