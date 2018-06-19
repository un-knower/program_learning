package demo.scala.akka

import akka.actor.Actor
import akka.actor.TypedActor.PreStart
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import akka.actor.Props

class Master extends Actor {  //Master Actor
  println("constructor invoked")
  def PreStart():Unit = {
      println("preStart invoked")
  }
    
    //用于接收消息
  def receive: Actor.Receive = { //type Receive = PartialFunction[Any, Unit]
      case "connect" => {
          println("a client connected")
      }
      case "hello" => {
          println("hello")
      }
    
  }
  
}

object Master {
    def main(args: Array[String]): Unit = {
        
//        val host = args(0)
//        val port = args(1).toInt
        val host = "172.21.75.240"
        val port = 8888
        
        //准备配置
        val configStr = s"""
              |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
              |akka.remote.netty.tcp.hostname = "$host"
              |akka.remote.netty.tcp.port = "$port"
          """.stripMargin //默认使用|切分属性。使用|是切分出来的每行数据对其
        val config = ConfigFactory.parseString(configStr) //从字符串中读取配置  //也可以parseFile 从文件中读取配置 
        //ActorSystem 老大，负责创建和监控下面的actor，是单例的
        val actorSystem = ActorSystem("MasterSystem",config)
        
        //创建actor
        val master = actorSystem.actorOf(Props[Master], "Master")
        
        //发送消息
        master ! "hello"
        
        actorSystem.awaitTermination()
        
        
    }
}











