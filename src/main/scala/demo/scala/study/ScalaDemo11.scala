package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo11 {
    /**
     * 一些常用的高阶函数
     */
    def main(args:Array[String]):Unit= {
        /**
         * map
         */
        print((1 to 9).map(0.1*_))
        /**
         * foreach
         */
        (1 to 9).map("*"*_).foreach ( println _ )  //foreach不返回任何值
        
        /**
         * filter
         */
        print( (1 to 9).filter(_%2==0) )    //Vector(2, 4, 6, 8)
        /**
         * reduceLeft
         */
        println
        print( (1 to 9).reduceLeft(_*_) )  //362880      1*2*3*4*5*6*7*8*9
        
        println
        /**
         * 
         */
        "Mary has a little lamb".split(" ").sortWith(_.length<_.length).foreach(println _)
//      a
//      has
//      Mary
//      lamb
//      little
        
        /**
         * 闭包
         */
        
        def mulBy(factor:Double)=(x:Double)=>{println(factor);factor*x}
        //考虑调用
        var triple=mulBy(3)
        println(triple)  //<function1> mulBy返回一个函数，赋值给triple
        var half=mulBy(0.5)
        println(triple(14)+" "+half(14))    //42.0 7.0
        //即每个返回函数都有自己的factor设置，scala编译器会确保你的函数可以访问非局部变量
        println(mulBy(11)(0.5))  //5.5
        
        /**
         * 柯里化
         * 指：原来接收两个参数的函数变成新的接收一个参数的函数的过程
         * 新的函数返回一个以原有第二个参数作为参数的函数
         */
        //如下函数接收两个参数
        def mul(x:Int,y:Int)=x*y
        //如下函数接收一个参数
        def mulOneAtATime(x:Int)=(y:Int)=>x*y
        
        //要计算两个数的乘积
        println( mulOneAtATime(6)(7) ) //42 //严格来讲，返回结果是 (y:Int)=>6*y
        
        //scala支持如下简写来定义这样的柯里化函数
        def mulOneAtATime2(x:Int)(y:Int)=x*y
        
    }
        
}