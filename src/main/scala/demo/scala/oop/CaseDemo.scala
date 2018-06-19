package demo.scala.oop

/**
 * @author qingjian
 */
//抽象类
abstract class Amount
//样例类
case class Dollar(value:Double) extends Amount
case class Currency(value:Double, unit:String) extends Amount

//样例对象
case object Nothing extends Amount




object CaseDemo {
    def main(args: Array[String]): Unit = {
        val amt = Currency(29.95,"EUR")      
        val price = amt.copy()  //复制
        println("amt="+amt.value+":"+amt.unit)
        println("price="+price.value+":"+price.unit)
      
        val price2 = amt.copy(unit="CHF")  //复制时，可以改变某个值
        println("price2="+price2.value+":"+price2.unit)
        
        
    }
}