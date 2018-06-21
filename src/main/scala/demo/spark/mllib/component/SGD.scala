package demo.spark.mllib.component

import scala.collection.mutable.HashMap

/**
 * 随机梯度下降算法
 */
object SGD {
  val data = HashMap[Int, Int]()
  def getData(): HashMap[Int, Int] = {
    for (i <- 1 to 50) {
      data += (i -> 2 * i)
    }
    data
  }

  var delta = 0.0
  var alpha = 0.1

  def sgd(x: Double, y: Double) = {
    delta = delta - alpha * (delta * x - y)
  }
  def main(args: Array[String]) {
    val dataSource = getData();
    dataSource.foreach(myMap => {
      sgd(myMap._1, myMap._2)
      
    })
    
    println("最终结果delta值为：" + delta)
  }
}