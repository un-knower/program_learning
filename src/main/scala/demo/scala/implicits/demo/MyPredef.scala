package demo.scala.implicits.demo

/**
 * @author qingjian
 */
class OrderedGirl {
    
}
object MyPredef {
  implicit def girl2OrderedGirl(g:Girl) = new Ordered[Girl] { //因为Ordered是一个trait，所以需要实现它的实现类
      def compare(that: Girl): Int= {
          if(g.faceValue == that.faceValue) {
              g.age - that.age
          }
          else {
              g.faceValue - that.faceValue
          }    
      }
  }
  //下面与上面实现结果一样
  implicit val girl2OrderedGirl2 = (g:Girl)=> new Ordered[Girl] {
     def compare(that: Girl): Int= {   //这个是由其中一个girl来调用，所以不需要隐式参数
          if(g.faceValue == that.faceValue) {
              g.age - that.age
          }
          else {
              g.faceValue - that.faceValue
          }   
      } 
  } 

  implicit val girl2OrderedGirl3 = (g:Girl)=> new Ordering[Girl] {
	  def compare(x: Girl, y: Girl): Int = { //看函数的参数，是两个（x，y），所以需要隐式参数来调用这个方法
           if(x.faceValue == y.faceValue) {
               x.age - y.age
           }
           else {
        	   x.faceValue - y.faceValue
           }
	  }  
  } 
  
  
  
}