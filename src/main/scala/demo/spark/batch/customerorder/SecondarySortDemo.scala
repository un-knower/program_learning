package demo.spark.batch.customerorder

import org.apache.spark.{SparkConf, SparkContext}

class SecondarySort(val first:Int, val second:Int) extends Ordered[SecondarySort] with Serializable {
  override def compare(that: SecondarySort): Int = {
      if ( this.first - that.first != 0 ) {
        this.first - that.first
      } else {
        this.second - that.second
      }
  }
}

/**
  *  sortByKey实现二次排序
  */
object SecondarySortDemo {
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]").setAppName("secondary sort")
    val sc = new SparkContext(conf)
    val data = sc.textFile("src\\main\\java\\demo\\hadoop\\sort\\secondarysort")
    val pairWithSortKey = data.map(line => {
      val splited = line.split(" ")
      (new SecondarySort(splited(0).toInt, splited(1).toInt), line)
    })

    // sortByKey实现二次排序
    val sorted = pairWithSortKey.sortByKey(false)  // 递减排序（默认递增）
    val sortedResult = sorted.map(sortedLine => sortedLine._2)


    sortedResult.collect.foreach(println)
//    sortedResult.foreach(println)


    println("*"*10)


    val  sortedResultWithIndex = sortedResult.mapPartitionsWithIndex((index:Int, iter:Iterator[String]) => {
      iter.toList.map(x => "partition"+index+" => "+x).iterator
    })

    sortedResultWithIndex.foreach(println)

    sc.stop()

  }
}
