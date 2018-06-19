package demo.scala.actor

import scala.actors.Actor

/**
 * @author qingjian
 */
//注意：发送start消息和stop的消息是异步的，但是Actor接收到消息执行的过程是同步的按顺序执行
class MyActor extends Actor {
  def act(): Unit = {
      while(true) {
          receive { //没有match有case ，是偏函数
              case "start" => {
                  println("start...")
                  Thread.sleep(5000)
                  println("started")
              }
              case "stop" => {
                  println("stopping...")
                  Thread.sleep(5000)
                  println("stopped")
                 
              }
              
              
          }
      }
  }
}
object MyActor {
    def main(args:Array[String])= {
        val actor = new MyActor()
        actor.start()
        actor ! "start"
        actor ! "stop"
        println("消息发送完成!") //异步执行，所以先打印 “消息发送完成！”
    }
}

//消息发送完成!
//start...
//started
//stopping...
//stopped