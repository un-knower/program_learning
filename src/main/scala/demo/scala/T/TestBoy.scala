package demo.scala.T

/**
 * @author qingjian
 */
object TestBoy {
    def main(args: Array[String]): Unit = {
        val b1 = new Boy("zhangsan", 88)
        val b2 = new Boy("lisi", 99)
        val b3 = new Boy("wangwu", 66)
        val arr = Array(b1,b2,b3)
//        val arrSorted = arr.sorted  / /因为实现了compareTo方法
//        val arrSorted = arr.sortBy { x => x.faceValue }
        val arrSorted = arr.sortBy { x => x } //因为实现了compareTo方法
        for(b <- arrSorted) {
            println(b.name)
        }
    }
  
}