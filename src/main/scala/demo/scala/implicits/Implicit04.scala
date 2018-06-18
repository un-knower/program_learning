package demo.scala.implicits

/*
 * 隐式参数
 */

class SignPen {
    def write(content:String) = println(content)
}
object ImplicitContext {
    implicit val signPen = new SignPen  //隐式参数，配合函数柯里化使用
}
object Implicit04 {
    def signForExam(name:String)(implicit signPen:SignPen):Unit = {
        signPen.write(name+" arrive in time.")
    }
    def main(args: Array[String]): Unit = {
        import ImplicitContext._
        //签名使用公共的一支笔
        signForExam("aaaaaa") //隐式参数自动传入隐式参数，到上下文去去找 implicit val且是SignPen的类型值
        signForExam("bbbbbb")
      
    }
  
}