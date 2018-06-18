package demo.scala.implicits.demo2

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

class Chooser1[T <% Ordered[T]] { //视图界定
  def choose(first:T, second:T):T= {
    if(first > second) first else second
  }

}

object Chooser1 {
  def main(args:Array[String]):Unit = {
        import MyPredef1._ 
        val c = new Chooser1[Girl1]
        val g1 = new Girl1("ab",90)
        val g2 = new Girl1("cd",99)
        val arr = Array(g1,g2)
        val g = c.choose(g1, g2)
        println(g.name)
    }
}
