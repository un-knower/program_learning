package demo.scala.study

import scala.util.Random

/**
 * @author qingjian
 */
//样例类是一种特殊的类，可用于模式匹配
//case class是多例的，后面要跟构造参数
//case object是单例的
case class SubmitTask(id:String, name:String)
case class HeartBeat(time:Long)
case object CheckTimeOutTask  //单例对象
//case object CheckTimeOutTask2() //报错 trait or objects may not have parameters
object CaseDemo4 extends App{
    val arr = Array(CheckTimeOutTask, HeartBeat(12333), SubmitTask("0001","task-0001"))
    arr(Random.nextInt(arr.length)) match {
        case SubmitTask(id,name) => {
            println(s"id = $id, name = $name")
        }
        case HeartBeat(time) => {
            println("time = "+ time)
        }
        case CheckTimeOutTask => {
            println("check")
            println(this) //scala.demo.ObjectClass.CaseDemo4$@17327b6
        }
        
    }
  
}