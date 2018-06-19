package demo.scala.implicits.demo2

/**
 * @author qingjian
 */
 
object MyPredef1 {
  implicit def girl2OrderedGirl(g:Girl1) = new Ordered[Girl1] { //因为Ordered是一个trait，所以需要实现它的实现类
      def compare(that: Girl1): Int= {
          g.faceValue - that.faceValue
      }
  }
  //下面与上面实现结果一样
  implicit val girl2OrderedGirl2 = (g:Girl1)=> new Ordered[Girl1] {
     def compare(that: Girl1): Int= {   //这个是由其中一个girl来调用，所以不需要隐式参数
          g.faceValue - that.faceValue
      } 
  } 

  
  
  
  
}