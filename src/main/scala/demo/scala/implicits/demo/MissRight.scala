package demo.scala.implicits.demo

/**
 * @author qingjian
 */
class MissRight[T] {
  //利用函数柯里化，相当于viewbound视图界定 将T隐式转换成Ordered[T]
  def choose(first:T, second:T)(implicit ord: T=>Ordered[T]):T = { //柯里化传入的是隐式函数
      if(first > second) first else second
  }
  
  def select(first:T, second:T)(implicit ord: Ordering[T]):T = {//柯里化传入的是隐式参数
      if(ord.gt(first, second)) first else second
  }
  def random(first:T, second:T)(implicit ord: Ordering[T]):T = {//柯里化传入的是隐式参数
      import Ordered.orderingToOrdered  //虽然传入的是Ordering，但是仍然想使用> = < ，可以再把Ordering转换成Ordered
      if(first > second) first else second
  }
  
}

object MissRight {
  def main(args: Array[String]): Unit = {
    val mr = new MissRight[Girl]
    val g1 = new Girl("a",11,22)
    val g2= new Girl("b",22,33)
    import MyPredef._
    val g = mr.select(g1, g2)
    println(g.name)
    
    
  } 
}