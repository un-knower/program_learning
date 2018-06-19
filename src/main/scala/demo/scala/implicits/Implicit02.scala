package demo.scala.implicits

/**
使用隐式转换加强现有类型
  超人变身（起到装饰类型的作用）
  类型没有方法的时候会尝试到上下文找能够隐式转换
 */
class Man(val name:String)
class Superman(val name:String) {
    def emitLaser = println("emit laser!")
}

object Implicit02 {
    implicit def man2Superman(m:Man) = new Superman(m.name)
    def main(args: Array[String]): Unit = {
       val man = new Man("man")
       man.emitLaser
       
    }
  
}