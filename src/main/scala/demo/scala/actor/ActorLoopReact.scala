package demo.scala.actor

import scala.actors.Actor

/**
 * @author qingjian
 */
//react方式会复用线程，比receive更高效；如果使用react方式循环执行消息处理，外层使用loop，不能使用while 
class ActorLoopReact extends Actor {
  def act(): Unit = {
      loop {
          react {
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
              case "exit" => {
                  println("exit")
                  exit()   
              }
          }
      }
  }
}
object ActorLoopReact {
    def main(args: Array[String]): Unit = {
        val actor = new ActorLoopReact
        actor.start()
        actor ! "start" //下面两句是串行的，不是并行的
        actor ! "stop"   //
        println("消息发送完成！")
        actor ! "exit"
        
    }
  
}