package demo.scala.implicits.demo3

/**
 * @author qingjian
 */
 
object MyPredef2 {
  
  implicit val girl2OrderingGirl2 = (g:Girl2) => new Ordering[Girl2] {
      def compare(x: Girl2, y: Girl2): Int = {
          x.faceValue - y.faceValue
      }
  }
  implicit def girl2OrderedGirl(g:Girl2) = new Ordered[Girl2] { //因为Ordered是一个trait，所以需要实现它的实现类
      def compare(that: Girl2): Int= {
          g.faceValue - that.faceValue
      }
  }
  //下面与上面实现结果一样
  implicit val girl2OrderedGirl2 = (g:Girl2)=> new Ordered[Girl2] {
     def compare(that: Girl2): Int= {   //这个是由其中一个girl来调用，所以不需要隐式参数
          g.faceValue - that.faceValue
      } 
  }
  
  
}