package demo.scala.actor

import scala.actors.Actor
import scala.io.Source
import scala.actors.Future
import scala.collection.mutable.ListBuffer

/**
 * @author qingjian
 */
case class SubmitTask(fileName:String) //提交任务封装类
case object StopTask //结束任务分装类
case class ResultTask(result:Map[String, Int]) //返回结果的封装类
class Task extends Actor {
  def act(): Unit = {
      loop {
          react {
              case SubmitTask(fileName) => {
                  //val content = Source.fromFile(fileName).mkString
                  //val arr = content.split("\r\n")
                  //val result = arr.flatMap(_.split(" ")).map((_,1)).groupBy(_._1).mapValues(_.length)
                  //sender ! ResultTask(result) //返回结果
                  //上面相当于
                  val arr = Source.fromFile(fileName).getLines()
                  val result = arr.flatMap(_.split(" ")).map((_,1)).toList.groupBy(_._1).mapValues(_.length)
                  sender ! ResultTask(result) //返回结果
              } 
              case StopTask => {
                  exit()
              }
              
          }
      }
  }
}


object ActorWordCount {
    def main(args: Array[String]): Unit = {
        val files = Array("src/Scala_Actor/words","src/Scala_Actor/words2")
        val replaySet = new scala.collection.mutable.HashSet[Future[Any]] //replay数组
        val resultList= new scala.collection.mutable.ListBuffer[ResultTask]
        for(f <- files) {
            val t = new Task //actor
            val replay = t.start() !! SubmitTask(f)
            replaySet+=(replay)
            
        }
        while(replaySet.size>0) {
            for(r<-replaySet) {
                println(r.isSet)
            }
            val toCompute = replaySet.filter(_.isSet) //过滤 返回出来得到结果的进程
            for(t<-toCompute) {
                val result:Any = t.apply() //拿到结果
                resultList += result.asInstanceOf[ResultTask] //Any类型强制转换成ResultSet
                replaySet.remove(t)
                println(replaySet.size)
            }
            Thread.sleep(100)
        }
        
        //val mapResult: ListBuffer[Map[String, Int]] = resultList.map(_.result)
        //val flt: ListBuffer[(String, Int)] = mapResult.flatten
        
        val result = resultList.map(_.result).flatten.groupBy(_._1).mapValues(x=>x.foldLeft(0)(_+_._2))
        println(result) //Map(world -> 1, qingjian -> 2, you -> 1, me -> 1, xyz -> 1, hello -> 5, lala -> 1)
        
    }
  
}