package demo.spark.batch.customerorder

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 自定义排序:二次排序
 * 比较规则： 先按faceValue排序，然后比较年龄
 * 
 * 第一种方法：
 * 
 * 先将数据name,faceValue,age,id 封装成Girl类
 * Girl类继承Sorted with Serializable
 * 使用rdd.sortBy(Girl类) 调用
 */

object CustomerOrdered {
    def main(args:Array[String]):Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("customer order")
        val sc = new SparkContext(conf)
        
        val data = sc.parallelize(List(("a",88,27,1),("b",98,26,2),("c",88,28,3),("d",88,18,3)))
         
        val sortedRdd = data.sortBy(x=>Girl(x._2, x._3))
        sortedRdd.collect.foreach(println)
//(d,88,18,3)
//(a,88,27,1)
//(c,88,28,3)
//(b,98,26,2)
        val sortedRdd2 = data.sortBy(x=>Girl(x._2, x._3),false)
        sortedRdd2.collect.foreach(println)
//(b,98,26,2)
//(c,88,28,3)
//(a,88,27,1)
//(d,88,18,3)       
    }
}

//自定义比较器：继承extends Ordered 和 with Serializable  因为数据存放在不同的机器上，所以需要数据传输，需要序列化
case class Girl(faceValue:Int, age:Int) extends Ordered[Girl] with Serializable {
  def compare(that: Girl): Int = {
      if(this.faceValue == that.faceValue) {
          this.age - that.age
      }else {
          this.faceValue - that.faceValue
      }
    
  }
}