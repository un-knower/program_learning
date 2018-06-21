package demo.spark.mllib.component

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RDDSample {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("sample").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val data = sc.parallelize(Array("a","b","c","d","e","f","g","h","i","g"))
        val dataS = data.sample(false,0.5d)
        dataS.foreach(println)  //返回5个，4个
        println("---------------")
        val dataS2 = data.sample(false,0.2d)
        dataS2.foreach(println)  //返回3个，2个
        
        println("---------------")
        val dataAll = data.sample(false, 1.0d)
        dataAll.foreach { println }
    }
   
}