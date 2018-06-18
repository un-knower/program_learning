package demo.scala.study

object ScalaDemo24 {
  def test(str:String*) {
    for(s<-str) {
        println(s)
    }
    println("---------")
    println(str(2))
      
  }
  def main(args:Array[String]):Unit = {
      val arr = Array("s1","s2","s3")
      test(arr:_*)
  }
  
  
}