package demo.scala.study

object ScalaDemo26 {
  def main(args: Array[String]): Unit = {
      
     //打印多少次ABC ？ 
     val t1,t2,(a,b,c) = {
         println("ABC")
         (1,2,3)
     }
     
     //因为赋值给t1,t2,(a,b,c)共3次，所以打印3次
     println("*"*8)
     println(t1)   //(1,2,3)
     println(t2)   //(1,2,3)
     println(a)    //1
     println(b)    //2
     println(c)    //3
     
     
  }
}