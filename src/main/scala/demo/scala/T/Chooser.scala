package demo.scala.T

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

class Chooser[T : Ordering] { //上下文界定
  def choose(first:T, second:T):T= {
    var ord = implicitly[Ordering[T]]
    if(ord.gt(first, second)) first else second
  }

}

object Chooser {
  def main(args:Array[String]):Unit = {
        import MyPredef._ 
        val c = new Chooser[Girl]
        val g1 = new Girl("ab",90)
        val g2 = new Girl("cd",99)
        val arr = Array(g1,g2)
        val g = c.choose(g1, g2)
        println(g.name)
    }
}
