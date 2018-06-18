package demo.scala.actor

import scala.io.Source

/**
 * @author qingjian
 */
object WordCount {
    def main(args: Array[String]): Unit = {
        val content = Source.fromFile("src/Scala_Actor/words").mkString
        val data = content.split("\r\n")
        val d: Map[String, Array[(String, Int)]] =data.flatMap(_.split(" ")).map((_,1)).groupBy(_._1)
        for(i<-d) {
            println(i._1+"->"+i._2.toList)
        }
        val result = d.map{ e =>(e._1,e._2.length)}
        println(result)
      
    }
}