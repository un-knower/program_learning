package demo.scala.actor

/**
 * @author qingjian
 */
//并发编程
import scala.actors.Actor //use akka actor instead
/**
 * 调用单例对象的start()方法，它们 的act方法会被执行
 * 实现多线程
 */
object MyActor1 extends Actor {
  //重新定义actor方法
  def act(): Unit = {
    for(i <- 1 to 10) {
        println("actor-1 "+i)
        Thread.sleep(1000)
    }
  }
}

object MyActor2 extends Actor {
  def act(): Unit = {
    for(i <- 1 to 10) {
        println("actor-2 "+i)
        Thread.sleep(1000)
    }
  }
}

object ActorTest extends App{
    MyActor1.start()
    MyActor2.start()
    println("main")
} 