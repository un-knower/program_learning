package demo.scala.actor

import scala.actors.Actor

/**
 * @author qingjian
 */
case class SyncMsg(id:Int, msg:String)
case class AsyncMsg(id:Int, msg:String)
case class ReplyMsg(id:Int, msg:String)

class ActorCaseclass extends Actor { //继承scala.actors.Actor
  def act(): Unit = {
      while(true) {
          receive {
              case "start" => println("starting...")
              case SyncMsg(id,msg) => {
                  println(id+" sync "+msg) //2 sync hello sync msg
                  Thread.sleep(5000)
                  sender ! ReplyMsg(3, "finish") //ReplyMsg(3,finish)
              }
              case AsyncMsg(id, msg) => {
            	  println(id+" async "+msg) //1 async hello async msg
            	  Thread.sleep(5000)
              }
              
          }         
      }
  }
}

object ActorCaseclass {
    def main(args: Array[String]): Unit = {
        val actor = new ActorCaseclass
        actor.start()
        //异步消息
        actor ! AsyncMsg(1,"hello async msg") //发送异步消息
        println("异步消息发送完成")
        //同步消息 reply和reply2是异步的并行的，不是串行的！！！
        val reply = actor !! SyncMsg(2,"hello sync msg") //发送同步消息
        println("reply.isSet :"+reply.isSet) //false//true if the future's result is available, false otherwise.是否有返回结果
        val reply2 = actor !! SyncMsg(3,"hello sync msg") //发送同步消息
        println("reply2.isSet :"+reply2.isSet) //false//true if the future's result is available, false otherwise.是否有返回结果
        //
        println("等待返回结果...") //执行完成功后，在下一句阻塞
        val c = reply.apply()  
        val c2 = reply2.apply() 
        println("返回结果到达")
        println("reply.isSet :"+reply.isSet) //true
        println("reply2.isSet :"+reply2.isSet) //true
        println(c)      //ReplyMsg(3,finish)   
        println(c2)      //ReplyMsg(3,finish)   
      
    }
  
}

/*
异步消息发送完成
1 async hello async msg
reply.isSet :false
reply2.isSet :false
等待返回结果...
2 sync hello sync msg
3 sync hello sync msg
返回结果到达
reply.isSet :true
reply2.isSet :true
ReplyMsg(3,finish)
ReplyMsg(3,finish)
*/