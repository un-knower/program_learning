package demo.scala.study

object ScalaDemo4 {
  def main(args:Array[String]):Unit={
    /**
     * 函数组合
     */
    def f(s:String)="f("+s+")"
    def g(s:String)="g("+s+")"
    
    //compose
    //compose 组合其他函数形成一个新的函数 f(g(x))
    
    val fComposeG = f _ compose g _  //<function1>
    println(fComposeG)
    println(fComposeG("yay"))  //f(g(yay))
    
    //andThen
    //andThen 和 compose很像，但是调用顺序是先调用第一个函数，然后调用第二个，即g(f(x))
    val fAndThenG = f _ andThen g _
    println(fAndThenG("yay"))  //g(f(yay))
    
    /**
     * PartialFunction 偏函数
     */
    
    val one:PartialFunction[Int,String]={case 1=>"one"}
    println( one.isDefinedAt(1) )  //true
    println( one.isDefinedAt(2) )  //false
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }  
}