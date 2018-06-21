package demo.spark.batch.customerorder

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 普通排序
  */
object SortDemo {
  def main(args:Array[String]) = {
    val conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("sort")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(5,10,3,5,6,12,1))
    val sortedData = data.sortBy(x => x, false)
    sortedData.collect.foreach(println)
    sc.stop
  }

}
