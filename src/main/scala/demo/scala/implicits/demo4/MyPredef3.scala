package demo.scala.implicits.demo4

/**
 * @author qingjian
 */
 
object MyPredef3 {
    
    trait girl2OrderingGirl2 extends Ordering[Girl3] {
      def compare(x: Girl3, y: Girl3): Int = {
          x.faceValue - y.faceValue
      }
    }
    implicit object Girl3 extends girl2OrderingGirl2
  
  
  
}