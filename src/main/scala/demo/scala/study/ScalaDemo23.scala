package demo.scala.study

/**
 * 叫名参数
 */
object ScalaDemo23 {
    
    
    
    def main(args: Array[String]): Unit = {
        var assertionsEnabled = true
        /**
         * 如果没有叫名参数
         * 参数类型是 ()=> Boolean
         */
        def myAssert(predicate:()=> Boolean)=
            if(assertionsEnabled && !predicate())
                throw new AssertionError
        /**
         * 叫名参数
         * 参数类型是 =>Boolean        
         */
        def byNameAssert(predicate: => Boolean) = 
            if(assertionsEnabled && !predicate)
                throw new AssertionError
        /**
         * booleanAssert
         *         
         */
        def booleanAssert(predicate:Boolean) = 
            if(assertionsEnabled && !predicate)
                throw new AssertionError
        //不使用叫名参数
        myAssert( () => 5>3 )
        //使用叫名参数
        byNameAssert(5>3)
        //
        booleanAssert(5>3)
        
        //booleanAssert的参数类型是Boolean，在booleanAssert(5>3)里括号中的表达式咸鱼booleanAssert的调用被评估
        //表达式5>3产生true，被传给booleanAssert
        //相对，byNameAssert的predicate参数的类型是=>Boolean，byNameAssert(5>3)括号里的表达式不是先于byNameAssert的调用被评估
        //而是代之先创建一个函数值，其apply方法被评估5>3,而这个函数值将被被传递给byNameAssert
        
        //因此这两种方式之间的差别，在于如果断言被禁用，你会看到booleanAssert括号里的表达式的某些副作用，而byNameAssertquery没有
        
        assertionsEnabled = false
        byNameAssert(2/0==0) //没有副作用
        booleanAssert(2/0==0) //有副作用
// Exception in thread "main" java.lang.ArithmeticException: / by zero
//	at Scala_Study.ScalaDemo23$.main(ScalaDemo23.scala:47)
//	at Scala_Study.ScalaDemo23.main(ScalaDemo23.scala)       
        
        
        
        
        
        
        
                
      
    }
  
}