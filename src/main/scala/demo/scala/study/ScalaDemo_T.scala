package demo.scala.study

/**
 * @author qingjian
 */
object ScalaDemo_T {
    /**
     * 泛型
     */
    class Pair[T,S](val first:T,val second:S)
    def main(args: Array[String]): Unit = {
        val p = new Pair(12,"String")  //Pair[Int, String]
        val p2 = new Pair[Any,Any](12,"String")
        /**
         * 泛型函数
         * 函数和方法也可以带类型参数
         */
        def getMiddle[T](a:Array[T])=a(a.length/2)
        
        
        /**
         * 类型变量界定
         * 
         */
        class Pair_Error[T](val first:T, val second:T) {
            //下面语句是错误的，因为并不能知道first是否有compareTo方法
            //def smaller = if(first.compareTo(second)<0) first else second  //错误
        }        
        //解决方法是添加一个上界 T<:Comparable[T]
        //这意味着T必须是Comparable[T]的子类
        //这样一来，我们可以实例化Pair[java.lang.String]，但不能实例化Pair[java.io.File]
        class Pair2[T<:Comparable[T]](val first:T,val second:T) {
            def smaller = if(first.compareTo(second)<0) first else second
        }        
        val p3 = new Pair2("Fred","Brook")
        println( p3.smaller )    //Brook
        
        //replace方法传入参数替换第一个
        //所以方法传入的参数类型必须是被替换参数的超类型 R    R>:T
        class Pair3[T](val first:T,val second:T) {
            def replaceFirst[R>:T](newFirst:R) = new Pair3[R](newFirst,second)
        }
        
        
        /**
         * 视图界定
         * class Pair[T<:Comparable[T]]
         * 意味着T必须是Comparable 的子集
         * 如果试着 new 一个Pair(4,2)，编译就说报错，因为Int不是Comparable[Int]的子类
         * Scala的Int类型并没有是吸纳Comparable。
         * 不过RichInt实现了Comparable[Int]，同时还有一个从Int到RichInt的隐式转换
         * 所以解决方法是 使用视图界定
         * class Pair[T <% Comparable[T]]
         */
        
        //使用Ordered特质会更好，它在Comparable的基础上额外提供了关系操作符
        class Pair4[T<%Ordered[T]](val first:T,val second:T) {
            def smaller = if(first<second) first else second
        }
        
        /**
         * 视图界定T<%V要求必须存在一个从T到V的隐式转换。上下文界定的形式为T:M
         * 其中M是另一个泛型类，它要求必须存在一个类型为M[T]的“隐式值”
         * class Pair[T:Ordering]
         */
        
        
        
        
        /**
         * 要实例化一个泛型的Array[T]，我们需要一个Manifest[T]对象
         * 
         */
        def makPair[T:Manifest](first:T,second:T) {
            val r = new Array[T](2);r(0)=first;r(0)=second;r
            
        }
        
        /**
         * 多重界定
         * T>:Lower <:Upper
         */
        
        
        
    }
    
    
}