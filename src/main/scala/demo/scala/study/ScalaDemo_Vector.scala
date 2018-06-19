package demo.scala.study

/**
 * @author qingjian
 */
object ScalaDemo_Vector {
  def main(args:Array[String]):Unit={
      var vector1 = Vector(1,2,3) 
      println(vector1)   //Vector(1, 2, 3)
      
      var vector2=vector1:+5    //右结合
      println(vector1)   //Vector(1, 2, 3)
      println(vector2)   //Vector(1, 2, 3, 5)
      
      vector2=0+:vector2    //左结合
      println(vector2)   //Vector(0, 1, 2, 3, 5)
      
      
      
      
  }
}