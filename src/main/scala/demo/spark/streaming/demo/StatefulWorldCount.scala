
package demo.spark.streaming.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
 //状态操作:相当于有一个全局变量进行统计
object StatefulWordlCount {
  def main(args: Array[String]): Unit = {
    //StateFul 需要定义的处理函数，第一个参数是本次进来的值，第二个是过去处理后保存的值
    //第一个参数表示updateStateByKey分组之后的某个key的value集合（Seq）
    // 例如分完词之后输入是 (hello,1)(tom,1)(hello,1)(tom,1)(hello,1)
    // 那么传入这个函数的第一个参数是 对于hello来说是(1,1,1)，对于tom来说是(1,1)  
    val updateFunc = (values:Seq[Int],state:Option[Int])=>{ 
      //Option是一个具有两个子类Some[T]和None的泛型类，用来表示“无值”的可能性
       val currentCount = values.foldLeft(0)(_+_) //求和
       val previousCount = state.getOrElse(0)
       Some(currentCount+previousCount)  //如果有值可以引用，就使用Some来包含这个值。
    }
    
    val conf= new SparkConf().setAppName("").setMaster("local[2]")
   // val sc = new SparkContext(conf)
    //创建StreamingContext
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.checkpoint(".")  //因为是有状态的，需要保存之前的信息，所以这里设定了checkpoint的目的是防止断电后内存数据丢失
    //这里因为没有设置checkpoint的时间间隔，所以会发现每一次数据块过来，即切分一次，产生一个.checkpoint文件
    
    //获取数据
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap {_.split(" ") }
    val wordCounts = words.map { x => (x,1) }
    //使用updateStateByKey来更新状态
    val stateDStream = wordCounts.updateStateByKey(updateFunc)
    stateDStream.print()
    ssc.start()
    ssc.awaitTermination()
    
  }
}