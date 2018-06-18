package demo.scala.study

/**
 * @author qingjian
 */
trait Animal { //相当于java的接口，需要extends，多个trait需要with
  def run() { //非抽象方法，java8中的接口中可以写非抽象方法
      println("animal run")
      
  }
      
}