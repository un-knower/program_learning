package demo.scala.T

/**
 * @author qingjian
 */
class Boy(val name:String, val faceValue:Int) extends Comparable[Boy] { //Boy与Boy进行比较
  def compareTo(o: Boy): Int = {
    this.faceValue - o.faceValue //默认是递增排序，那么faceValue高的在后面
  }
}