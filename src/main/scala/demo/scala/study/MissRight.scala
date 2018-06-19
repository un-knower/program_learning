package demo.scala.study

import scala.io.Source

class MissRight { //MissRight() 括号可以不写
  //主构造器，在new的 时候主构造器里的内容会被执行 
  val name = "abc"
  println(name)
  
  
  def sayHi = { //只会加载该方法的定义，该方法不会被调用
      println("hi")
  }
  sayHi  //hi
  
  //读取文件
  try {
    val data = Source.fromFile("src/Scala_Study/MissRight.scala").mkString
    println(data)
  } catch {
    case t: Throwable => t.printStackTrace() // TODO: handle error
  } finally {
    println("finally")
  }
 
}


object MissRight {
  def main(args: Array[String]): Unit = {
    val m = new MissRight
    
  }
}