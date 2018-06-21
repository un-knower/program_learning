package demo.spark.batch.customerorder

import org.apache.spark.{SparkConf, SparkContext}


/**
  * groupByKey实现二次排序
  */
object SecondarySortDemo2 {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]").setAppName("secondary sort")
    val sc = new SparkContext(conf)
    val data = sc.textFile("src\\main\\java\\demo\\hadoop\\sort\\secondarysort")
    val pairWithSortKey = data.map(line => {
      val splited = line.split(" ")
      (splited(0).toInt, splited(1).toInt)
    })

    // groupByKey实现二次排序
    val sorted = pairWithSortKey.groupByKey().map(x => (x._1, x._2.toList.sortWith( _ > _)))  // 递减排序

    sorted.collect.foreach(println)

    sc.stop()

  }
}
