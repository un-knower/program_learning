package demo.scala.actor

import scala.actors.Actor
/*
Actor之间相互收发消息
 */
case class Message(info:String, sender:Actor)

class A(val other:Actor) extends Actor {
  other ! Message("I'm A",this) //向other发送消息
  def act(): Unit = {
    while(true) {
        receive {
            case reply:String => println("A: A receive "+reply)
            
        }
    }
  }
}

class B extends Actor {
  def act(): Unit = {
      loop {
          react {
              case Message(info, sender) => {
                  println("B: B receive "+info)
                  sender ! "hi, my name is B"
                  
              }
          }
      }
  }
}




object ActorToActor {
    def main(args: Array[String]): Unit = {
        val Bactor = new B
        val Aactor = new A(Bactor)
        Bactor.start()
        Aactor.start
    }
}

//B: B receive I'm A
//A: A receive hi, my name is B


