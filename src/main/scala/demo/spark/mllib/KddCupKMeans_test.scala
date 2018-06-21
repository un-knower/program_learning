package demo.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

/**
 * 网络入侵异常检测
 * kmeans
 */
object KddCupKMeans_test {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Kmean").setMaster("local")
        val sc = new SparkContext(conf)
        val data = sc.parallelize(Seq(("A","a"),("B","b"),("A","a"),("A","b")))
        data.countByKey().foreach(println) //统计key出现的次数
//(B,1)
//(A,3)       
        data.countByValue.foreach(println) //统计整体出现的次数
//((B,b),1)
//((A,a),2)
//((A,b),1)       
        val data2 = sc.parallelize(Seq("a","b","c","c","a"))
        data2.countByValue().foreach(println)
//(a,2)
//(b,1)
//(c,2)       
        val list = List(1,2,3)
        val combs = list.combinations(2)  //n维排列组合。An Iterator which traverses the possible n-element combinations of this list.
        combs.foreach (println)
//List(1, 2)
//List(1, 3)
//List(2, 3)       
        println("-"*8)        
        val list2 = List(1,3,1,2)
        list2.combinations(2).foreach(println)
//List(1, 1)
//List(1, 3)
//List(1, 2)
//List(3, 2)
        
        
        
        
    }
}