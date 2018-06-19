package demo.scala.implicits.demo3

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

class Chooser2[T : Ordering] { //上下文界定
  def choose(first:T, second:T):T= {
    var ord = implicitly[Ordering[T]]//隐式参数
    if(ord.gt(first, second)) first else second
  }

}

object Chooser2 {
  def main(args:Array[String]):Unit = {
        import MyPredef2._ 
        val c = new Chooser2[Girl2]
        val g1 = new Girl2("ab", 90)
        val g2 = new Girl2("cd", 99)
        val arr = Array(g1,g2)
        val g = c.choose(g1, g2)
        println(g.name)
    }
}
