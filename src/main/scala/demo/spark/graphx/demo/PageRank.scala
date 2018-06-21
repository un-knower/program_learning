package demo.spark.graphx.demo

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

/**
 * @author qingjian
 */
object PageRank {
  def main(args:Array[String]):Unit={
    //屏蔽日志
    Logger.getLogger("org.apach.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    
    //设置运行环境
    //配置
    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    val sc = new SparkContext(conf)
    
    //读入数据文件
    //边
    val links = sc.textFile("file/graphx-wiki-edges.txt")
    //顶点
    val articles:RDD[String] = sc.textFile("file/graphx-wiki-vertices.txt")
    
    //转载顶点和边
    val vertices = articles.map { line => 
      val fields = line.split("\t")
      (fields(0).toLong,fields(1))   //顶点id，顶点属性 //extends RDD[(VertexId, VD)](sc, deps)
    }
    
    val edges = links.map { line =>  
      val fields = line.split("\t")
      Edge(fields(0).toLong,fields(1).toLong, 0) //源顶点id，目标顶点id，边属性 //extends RDD[Edge[ED]](sc, deps) 
    }
    
    
    //cache操作
    val graph = Graph(vertices,edges,"").persist()
    
    //测试
    println("**********************************************************")
    println("获取5个triplet信息")
    println("**********************************************************")
    graph.triplets.take(5).foreach(println(_))

    //pageRank算法里面的时候使用了cache()，故前面persist的时候只能使用MEMORY_ONLY
    println("**********************************************************")
    println("PageRank计算，获取最有价值的数据")
    println("**********************************************************")
    val prGraph = graph.pageRank(0.001).cache()

    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    titleAndPrGraph.vertices.top(10) {
      Ordering.by((entry: (Long, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))

    sc.stop()
    
  }
  
}