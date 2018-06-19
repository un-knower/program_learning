package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo10 {
    /**
     * 高阶函数
     */
    
    def main(args: Array[String]): Unit = {
        /*
         * 作为值的函数
         */
        import scala.math._
        val num = 3.14  //将num设为3.14
        val fun = ceil _ //将fun设为ceil函数
        //ceil函数后的_意味着你确实指的是这个函数，而不是碰巧忘记了给它送参数
        //fun的类型为  (Double)=>Double
        //          参数                    返回值                    
        /*
         * 函数的操作：
         * 1.调用它
         * 2.传递它，存放在变量中，或者作为参数传递给另一个函数
         */
        //调用
        println( fun(num) )  //4.0
        //传递
        Array(3.14,1.42,2.0).map(fun)  //Array(4.0,2.0,2.0)
        Array(3.14,1.42,2.0).map(ceil)  //Array(4.0,2.0,2.0)
        
        
        /**
         * 匿名函数
         * 不需要给每一个函数命名
         */
        //匿名函数
        (x:Int) => 3*x
        //也可以将该匿名函数存放在变量中
        val triple = (x: Int)=> 3*x
        //与下面语句def一样
        def triple2 (x:Int)={
            3*x
        }
        def triple3(x:Int)=3*x
        
        //
        Array(3.14,1.42,2.0).map((x:Double)=>3*x)
        //也可以用{}
        Array(3.14,1.42,2.0).map{(x:Double)=>3*x}
        
        /**
         * 带函数参数的函数
         */
        //这里参数可以是任何接收Double并返回Double的函数
        def valueAtOneQuarter(f:(Double)=>Double)=f(0.25)
        //valueAtOneQuarter类型是什么？
        //类型可以写作   (参数类型)=>结果类型
        // ((Double)=>Double)=>Double
        valueAtOneQuarter(ceil _)
        valueAtOneQuarter(sqrt _)
        
        //由于valueAtOneQuarter是一个接收函数参数的函数，因此它被称作高阶函数
        
        //高阶函数也可以产出另一个函数
        def mulBy(factor:Double)=(x:Double)=>factor*x
        //它的类型是  (Double)=>((Double)=>Double)
        
        val qunituple = mulBy(5) //此时qunituple为5*x
        qunituple(20)  //此时5*20
        
        /**
         * 参数类型推断  
         */
        
        //当将一个匿名函数传递个另一个函数或方法时，Scala会尽可能帮助你推断出类型的信息
        valueAtOneQuarter ( (x:Double) => 3*x )
        //由于valueAtOneQuarter方法知道你会传入一个类型为(Double)=>Double的函数，所以可以简单写成
        valueAtOneQuarter ( x => 3*x )
        
        //如果参数在=>右侧只出现一次，可以用_替换掉它
        valueAtOneQuarter(3*_)
        
        //以上这些简写方式仅在参数类型已知的情况下有效
        //val fun = 3*_ //错误，无法推断出类型
        val fun = 3* (_:Double)
        val fun2:(Double)=>Double=3*_
        
        
    }
    
    
}