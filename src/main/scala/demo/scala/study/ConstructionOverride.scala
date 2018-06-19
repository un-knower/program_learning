package demo.scala.study

/**
 * @author qingjian
 */
class ConstructionOverride(val id:String) {
  var name:String = _
  def this(id:String, name:String) {
      this(id) //辅助构造器一定要先调用主构造,必须在第一行             this()
      this.name = name
      println("class construct")
  }
}


object ConstructionOverride {
   val age = 11
   def apply():ConstructionOverride = {
      println("apply")
      new ConstructionOverride("111")
   }
   def main(args: Array[String]): Unit = {
	   println("*"*10)
       val c = ConstructionOverride//静态对象
       c.age
       println("*"*10)
       val c2 = ConstructionOverride()//apply 只是调用object的apply方法
       //没有c2.age，
       println("*"*10)
       val c3 = new ConstructionOverride("")   //      
	   println("*"*10)
	   val c4 = new ConstructionOverride("","c4")  //class construct       
	   println(c4.name) //c4
	   
	   
	   
	   
   }
}