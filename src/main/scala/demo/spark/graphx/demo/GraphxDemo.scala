//package demo.spark.graphx.demo
//
//
///**
// * @author qingjian
// */
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.graphx.Edge
//import org.apache.spark.graphx.Graph
//import org.apache.spark.graphx.VertexId
//
///**
// * @author qingjian
// */
//object GraphDemo {
//  def main(args: Array[String]): Unit = {
//      //
//      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//      //
//      val conf = new SparkConf().setAppName("").setMaster("local[4]")
//      val sc = new SparkContext(conf)
//
//      //顶点
//      val vertextArray = Array(
//          (1L,("Alice",28)),
//          (2L,("Bob",27)),
//          (3L,("Charlie",65)),
//          (4L,("David",42)),
//          (5L,("Ed",55)),
//          (6L,("Fran",50))
//      )
//      //边
//      val edgeArray = Array(
//          Edge(2L,1L,7),
//          Edge(2L,4L,2),
//          Edge(3L,2L,4),
//          Edge(3L,6L,3),
//          Edge(4L,1L,1),
//          Edge(5L,2L,2),
//          Edge(5L,3L,8),
//          Edge(5L,6L,3)
//      )
//      //顶点rdd
//      val vertextRdd = sc.parallelize(vertextArray)
//      //边rdd
//      val edgeRdd = sc.parallelize(edgeArray)
//
//      //构建图
//      val graph: Graph[(String, Int), Int] = Graph(vertextRdd,edgeRdd)
//
//      //找出年龄大于30的顶点
//      graph.vertices.filter(_._2._2 > 30).collect().foreach{
//          case (id,(name,age))=>println(s"$name is $age")
//      }
//
//
//      //边操作：找出图中属性大于5的边
//      graph.edges.filter(_.attr > 5).collect().foreach {x=>
//          println(s"${x.srcId} to ${x.dstId} is ${x.attr}" )
//
//      }
//
//
//
//      //列出所有的tripltes
//
//      graph.triplets.collect().foreach(print _)
//
//      graph.triplets.collect().foreach{ trip=>
//          println(trip.srcAttr+" -"+ trip.attr+"-> "+trip.dstAttr)
//
//      }
//
//
//
//      //列出边属性>5的tripltes
//      graph.triplets.filter(_.attr>5).collect().foreach {x=>
//          println(x.srcAttr._1+" -"+x.attr+"-> "+x.dstAttr._1)
//
//      }
//
//
//
//      //找出图中最大的出度、入度、度数：
//      println("outDegrees")
//      println(graph.outDegrees) //VertexRDDImpl[26] at RDD at VertexRDD.scala:57
//      def maxDegree(a:(VertexId, Int),b:(VertexId, Int))= {
//          if(a._2>=b._2) {
//              a
//          }else {
//              b
//          }
//      }
//      val maxOutDegreeVertex = graph.outDegrees.reduce(maxDegree)
//      val maxOutDegreeVertexs = graph.outDegrees.filter(x=>x._2==maxOutDegreeVertex._2)
//      val maxInDegreeVertex = graph.inDegrees.reduce(maxDegree)
//      val maxInDegreeVertexs = graph.inDegrees.filter(x=>x._2==maxInDegreeVertex._2)
//
//      print("max out degree vertexs:")
//      maxOutDegreeVertexs.collect().foreach(println)
//      print("max in degree vertexs:")
//      maxInDegreeVertexs.collect().foreach(println)
//
//
//
//      /*****************转换操作******************/
//      graph.mapVertices{ case((id,(name,age)))=> (id,(name,age+10))}.vertices.collect().foreach(println)
//
//      graph.mapEdges(e=>e.attr*2).edges.collect().foreach(println)
//
//
//
//      /******************结构操作***************/
//      println("顶点年纪大于30的子图")
//      val subGraph = graph.subgraph(vpred= (id,vd)=>vd._2>=30)
//      subGraph.vertices.collect().foreach(println)
//      subGraph.edges.collect().foreach(println)
//
//      /*****************连接操作**************/
//      case class User(name:String, age:Int, inDeg:Int, outDeg:Int)
//      //创建一个新图，顶点VD的数据类型为User，并从graph做类型转换
//      val initialUserGraph = graph.mapVertices{case(id,(name,age))=>User(name,age,0,0)}
//      //initialUserGraph与inDegrees、outDegrees（RDD）进行连接，并修改initialUserGraph中inDeg值、outDeg值
//      val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
//          case(id,u,inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
//      }.outerJoinVertices(initialUserGraph.outDegrees) {
//          case(id,u,outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
//      }
//      userGraph.vertices.collect().foreach(println)
//
//      //连接图的属性
//      println("连接图的属性")
//      userGraph.vertices.collect().foreach{u=>
//          println(s"name=${u._2.name},age=${u._2.age},inDegree=${u._2.inDeg},outDegree=${u._2.outDeg}")
//      }
//      println("入度和出度相同的人员")
//      userGraph.vertices.filter(x=>x._2.inDeg == x._2.outDeg).collect.foreach(println)
//
//
//      /***********聚合操作*****************/
//      println("user graph:")
//      userGraph.vertices.collect().foreach(println)
//      println("年纪最大的追求者")
//      val oldestFollower =userGraph.mapReduceTriplets[(String,Int)](//[A]
//          //将源顶点的属性发送给目标顶点，map过程
//          //                 (分组id,这里按dstId进行分组聚合 (   String     ,        Int          )A
//          edgeTrip=>Iterator((edgeTrip.dstId,(edgeTrip.srcAttr.name, edgeTrip.srcAttr.age))),
//          //得到最大追求者
//          (a, b)=>if(a._2>b._2) a else b  //(A, A)=>A
//      )
//
//
//      oldestFollower.collect().foreach(println)
//
//      println("*"*10)
//      userGraph.vertices.leftJoin(oldestFollower) { (id, user, optOldestFollower) => //按id进行left join
//      optOldestFollower match {
//        case None => s"${user.name} does not have any followers."
//        case Some((name, age)) => s"${name} is the oldest follower of ${user.name}."
//      }
//    }.collect.foreach { case (id, str) => println(id+":"+str)}
//    println
//    println("**********************************************************")
//    println("聚合操作")
//    println("**********************************************************")
//    println("找出5到各顶点的最短：")
//    val sourceId = 5L
//    val initialGraph: Graph[Double, Int] = graph.mapVertices((id,_)=> if(id==sourceId) 0.0 else Double.PositiveInfinity)
//    val sssp = initialGraph.pregel(Double.PositiveInfinity)( //第一个括号：第一次迭代的初始值（分发到各个节点的初始值）
//            (vertextid,dist,newDist) => math.min(dist, newDist),  //(vertextid,vd,第一次迭代的初始值（分发到各个节点的初始值）)=>vd2
//            triplet=> {
//                if (triplet.srcAttr + triplet.attr < triplet.dstAttr) { //如果源节点的距离+边属性（距离）<目标节点的距离 //是个map的过程
//                  Iterator((triplet.dstId, triplet.srcAttr + triplet.attr)) //Iterator((vertexid,attr)) //更新目标节点的VD（距离）
//                } else {
//                  Iterator.empty //否则的话不更新
//                }
//            },
//            (a,b)=>math.min(a,b) //每个节点VD合并merge处理//是个reduce的过程
//        )
//    println(sssp.vertices.collect().mkString("\n"))
//
//    sc.stop()
//  }
//}