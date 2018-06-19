package demo.scala.implicits.demo4

/**
 * Created with IntelliJ IDEA.
 * User: qingjian
 * Date: 16-11-21
 * Time: 下午5:33
 * To change this template use File | Settings | File Templates.
 */
//class Chooser[T <% Ordered[T]] {
//  def choose(first:T, second:T):T= {
//    if(first>second) first else second
//  }
//
//}

class Chooser3[T : Ordering] { //上下文界定
  def choose(first:T, second:T):T= {
    var ord = implicitly[Ordering[T]]//隐式参数
    if(ord.gt(first, second)) first else second
  }

}

object Chooser3 {
  def main(args:Array[String]):Unit = {
        import MyPredef3._ 
        val c = new Chooser3[Girl3]
        val g1 = new Girl3("ab", 90)
        val g2 = new Girl3("cd", 99)
        val arr = Array(g1,g2)
        val g = c.choose(g1, g2)
        println(g.name)
    }
}
