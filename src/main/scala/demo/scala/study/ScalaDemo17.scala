package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo17 {
  /**
   * 类型参数
   * 
   */
    
    
    
    
    def main(args: Array[String]): Unit = {
        //泛型类
        //两个类型参数T和S的类
   		class Pair[T, S](val first:T, val second:S) //类参数可以是任意(arbitrary)的名字。用[]来包围，而不是用()来包围，用以和值参数进行区别。
   		val p = new Pair(42,"String") //Pair[Int, String]
        
        val p2 = new Pair[Any, Any](42, "String")
        
        
        //泛型函数
        def getMiddle[T](a:Array[T]) = a(a.length/2)  //函数参数类型用[]包围
        
        println( getMiddle(Array("Mary", "had", "a", "little", "lamb")) ) //a
        
        val f = getMiddle[String] _
        println(f(Array("Mary", "had", "a", "little", "lamb")))   //a
        
        
        //如果添加方法，要产生较小的那个值
        /*class Pair2[T](val first:T, val second:T){
            def smaller = if(first.compareTo(second)<0) first else second //错误
        }*/
        //因为我们并不知道first是否有compareTo方法，可以使用T<:Comparable[T]，表示T为Comparable[T]子类
        class Pair2[T<:Comparable[T]](val first:T, val second:T){
            def smaller = if(first.compareTo(second)<0) first else second //错误
           
        }
        
        //父类去替换子类
        class Pair3[T](val first:T, val second:T) {
             def replaceFirst[R >: T](newFirst:R) = new Pair3[R](newFirst, second) //上界
        }
        
        
        //视图界定
        //class Pair[T <: Comparable[T]] 如果试着new Pair(4,2)，则会报错，因为Int不是Comparable[T]子类
        //解决方法是使用“视图界定”
        // class Pair[T <% Comparable[T]]    
        // <% 意味着T可以被隐式转换成Comparable[T]
        
        
        
        //上下文界定
        //视图界定  T<%V 要求必须存在一个从T到V的隐式转换。
        //上下文界定的形式为 T:M，其中M是另一个泛型，它要求必须存在一个类型为M[T]的“隐式值”
        class Pair4[T:Ordering](val first:T, val second:T) {
            def smaller(implicit ord:Ordering[T])=
                if (ord.compare(first, second)<0) first else second
            
        }
        
        
        //Manifest上下文界定
        def makePair[T:Manifest](first:T, second:T) {
            val r = new Array[T](2);r(0)=first;r(1)=second;r
            
        }
        
        makePair(4,9) //将定位到隐式Manifest[Int]并实际上调用makPair(4,9)(intManifest)
        
        
        
        //多重界定
        //T>:Lower<:Upper
        //T<:Comparable[T] with Serializable with Cloneable
        //多个视图界定
        //T<%Comparable[T]<%String
        //多个上下文界定
        //T<:Ordering<:Manifest
        
        //类型约束
        //T=:=U
        //T<:<U
        //T<%<U
        
        
        
        
    }
    
    
    
    
    
}