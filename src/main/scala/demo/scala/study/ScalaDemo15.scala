package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo15 {
    /**
     * 模式匹配机制
     */
    
    /**
     * Scala模式匹配并不会有“意外调入到下一个分支”的问题
     */
    def main(args:Array[String]):Unit= {
        
   		var sign=0
 		val ch:Char='+'
    	ch match {
    		case '+'=> sign = 1
    	    case '-'=> sign = -1
    		case '_'=> sign= -2   //default
    				
    	}
    	println( sign )    //1
        
      /**
       * 类型模式
       */
//        var obj:Int=1
//        obj match {
//            case x: Int=>x
//            case s: String => Integer.parseInt(s)
//            case _: BigInt => Int.MaxValue
//            case _=> 0
//            
//        }
        
        
        val pair=(1,0)
        pair match{
            case (0,_) => println("0...")   
            case (y,_) => println(y+"...0")   //1...0
            case _=> println("neither is 0")   
        }
        
        /**
         * copy方法和带名参数
         * 样例类的copy方法创建一个与现有对象值相同的新对象
         */
        
        
        case class Obj(name:String,age:Int) {
            
        }
        val amt = new Obj("EUR",29)
        val amt2 = amt.copy()
        println(amt.name+":"+amt.age)    //EUR:29
        println(amt2.name+":"+amt2.age)    //EUR:29
        
        
        
        /*
         * sealed
         * sealed 密封类
         * 保证了密封类的所有子类都必须在该密封类相同的文件中定义。
         */
        sealed abstract class Amount
        case class Dollar(value:Double) extends Amount
        case class Current(value:Double,unit:String) extends Amount
        
        /**
         * 偏函数
         * 被抱在花括号内的一组case语句是一个偏函数---并非对所有输入值都有定义的函数
         * 它是PartialFunction[A,B]类的一个实例   A是参数类型，B是返回类型
         */
        val f:PartialFunction[Char,Int]={case '+'=>1;case '-'=> -1}
        println( f('-') ) //-1
        //f('-')
        println( f.isDefinedAt('0') ) //false
        //println( f('0') ) //报错
        
        //collect方法将一个偏函数应用到所有该偏函数有定义的元素
        println( "-3+4".collect{case '+'=>1;case '-'=> -1} )    //Vector(-1, 1)
        
        
        
        
        
        
        
    }
    
  
}