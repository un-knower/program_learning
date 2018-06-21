package demo.spark.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

/**
 * Created with IntelliJ IDEA.
 * User: qingjian
 * Date: 16-2-21
 * Time: 下午5:35
 * Kmeans聚类
 * //随机选择K个中心点
 * //计算所有点到这K个中心点的距离，选择距离最近的中心点为其所在的簇
 * //简单的采用算术平均(mean)来重新计算K个簇的中心
 * //重复步骤2和3，直至簇类不再发生变化或者达到最大迭代值
 * //输出结果
 */
object Kmeans {
   def main(args:Array[String]) {
      //屏蔽不必要的日志显示在终端上
     Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
     Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

     //设置运行环境
     val conf = new SparkConf().setAppName("Kmean").setMaster("local")
     val sc = new SparkContext(conf)

     //装载数据
     val data = sc.textFile("C:\\Users\\qingjian\\Desktop\\test2.txt")
     val parseData = data.map(s=>Vectors.dense(s.split("\\s+").map(_.toDouble)))
     
     
     //强数据集聚类，2个类，20次迭代，进行模型训练形成数据模型
     val numClusters = 1
     val numIterations = 20
     val model = KMeans.train(parseData, numClusters, numIterations)
   
     //打印数模型的中心点
     println("Cluster centers:")
     for(c<-model.clusterCenters) {
       println(" "+c.toString)
     }
     
     val sumcost = model.computeCost(parseData);
     println("Within Set Sum of Squared Errors = " + sumcost)


   }
}
