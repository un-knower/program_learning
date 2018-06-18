package demo.scala.T

/**
 * @author qingjian
 */
class Pair[T <: Comparable[T]] { //表明T为Comparable的子类
  def bigger(first:T, second: T):T= {
      if( first.compareTo(second) > 0) first else second
  }
}

object Pair {
    def main(args: Array[String]): Unit = {
       val p = new Pair[String]
       val res = p.bigger("hadoop", "spark")
       println(res)
       val p2 = new Pair[Integer]
	   val res2 = p2.bigger(1, 2)
       println(res2)
    }
}
