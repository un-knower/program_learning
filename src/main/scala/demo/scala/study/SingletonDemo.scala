package demo.scala.study

object SingletonDemo {
    def main(args: Array[String]): Unit = {
        val s = SingletonDemo
        
        //Thread.sleep(100000) //100s
        
        val b = SingletonDemo
        
        println(s)
        println(b)  //二者是一样的
        
    }
  
}