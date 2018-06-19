package demo.scala.study

import scala.actors.Actor



/**
 * @author guangliang

 */
/**
 * act方法与java中的Runnable接口中的run方法很相似
 * 不同actor的act方法是并行运行的
 * 使用start方法启动
 */
class HiActor extends Actor {
    def act(){
        while(true) {
            receive {
                case "Hi" => println("Hello")
                
                
            }            
            
        }
        
    }    
}


case class Charge(creditCarnumber:Long, merchant:String, amount:Double);

object ScalaDemo20 {
    def main(args: Array[String]): Unit = {
        val actor1 = new HiActor
        actor1.start()
        
       
        //可以使用Actor的伴生对象来创建和启动actor
        val actor2 = Actor.actor{
            while(true) {
                Actor.receive{
                     case "Hi" => println("Hello")
                }
            }
        }
        actor2.start()
       //
        println("-----------------------")
        //所要发送的actor ! 发送消息
        //
        actor1!"Hi"  //Hello
        
        //也可以发送一个样例类
        
        
    }
}