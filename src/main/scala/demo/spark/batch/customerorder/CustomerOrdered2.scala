package demo.spark.batch.customerorder

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 自定义排序:
 * 比较规则： 先按faceValue排序，然后比较年龄
 * 
 * 第二种方法：使用隐式转换
 * 
 * 先将数据name,faceValue,age,id 封装成Girl类
 * Girl类继承Sorted with Serializable
 * 使用rdd.sortBy(Girl类) 调用
 */

//要写在上面，写在下面报错。。。
//隐式转换写在object中
object OrderContext {
    //implicit object BoyOrdering extends Ordering[Boy] {
    //或者下句
    implicit val boyOrdering = new Ordering[Boy] {
      def compare(x: Boy, y: Boy): Int = {
        if(x.faceValue > y.faceValue) 1 //小的排前面
        else if(x.faceValue == y.faceValue) {
            if(x.age > y.age) -1 else 1 //大的排前面
        }else -1
            
      }
    }
}

object CustomerOrdered2 {
    def main(args:Array[String]):Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("customer order")
        val sc = new SparkContext(conf)
        
        val data = sc.parallelize(List(("a",88,27,1),("b",98,26,2),("c",88,28,3),("d",88,18,3)))
        import OrderContext._
        val sortedRdd = data.sortBy( b => Boy(b._2, b._3))
        
        sortedRdd.collect.foreach(println)
        sc.stop()
    }
}

//自定义比较器：继承extends Serializable  因为数据存放在不同的机器上，所以需要数据传输，需要序列化
case class Boy(faceValue:Int, age:Int) extends  Serializable
