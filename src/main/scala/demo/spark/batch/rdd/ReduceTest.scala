package demo.spark.batch.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ReduceTest {
    
    def main(args: Array[String]): Unit = {
      def myfunc(index:Int, iter:Iterator[Int]):Iterator[String]= {
          iter.toList.map(x=>"[partID:"+index+",val:"+x+"]").iterator          
      }
        
        
      val conf = new SparkConf().setAppName("").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val z = sc.parallelize(1 to 10, 3)
       
//        val res = z.treeReduce(_+_) //55
//        println(res)
//        
//        val res2 = z.treeReduce(_-_) 
//        println(res2) //有时候17有时候23有时候-9
        
        val i = z.mapPartitionsWithIndex(myfunc)
        i.foreach(println)
        val res22 = z.reduce(_-_)
        println(res22) //有时候17有时候23有时候-9
        
//[partID:2,val:7]
//[partID:2,val:8]
//[partID:2,val:9]
//[partID:2,val:10]
//[partID:0,val:1]
//[partID:0,val:2]
//[partID:0,val:3]
//[partID:1,val:4]
//[partID:1,val:5]
//[partID:1,val:6] 
//17        
        

//[partID:1,val:4]
//[partID:2,val:7]
//[partID:0,val:1]
//[partID:2,val:8]
//[partID:1,val:5]
//[partID:2,val:9]
//[partID:0,val:2]
//[partID:2,val:10]
//[partID:1,val:6]
//[partID:0,val:3]
//23        
        
        
    }
  
}