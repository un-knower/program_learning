package demo.scala.study

/**
 * @author qingjian
 * Scala数据类型中的Symbol（符号文本）
 *  1.属于基本类型，被映射成scala.Symbol
    2.当两个Symbol值相等时，指向同一个实例
    3.Symbol类型存在的意义：作为不可变的字符串，同时不必重复地为相同对象创建实例，节省资源。这类似ruby的设计。
    4.定义变量val s=‘my_symbol时，s值即为
        s:Symbol = ’my_symbol。
            而s.name为Sting类型，值为my_symbol
 */
object ScalaDemo6_1 {
  def main(args: Array[String]): Unit = {
    //符号文本
    val s='my_symbol
    println(s)
    println(s.name)  //my_symbol
    val s1="my_symbol"
    //eq比较是否指向同一个对象
    println(s.eq(s1))    //false
    val s2='my_symbol
    println(s.eq(s2))   //true   作为不可变的字符串，同时不必重复地为相同对象创建实例，节省资源。这类似ruby的设计。
    
    
    //前缀操作符  - ! ~
    println(   -2.0   ) //实际上调用了 unary_
    println(   (2.0).unary_-   )
   
    //中缀操作符 *  - +   ||   &&等
    println(  2.0*3  )
    
    //后缀操作符
    //后缀操作符是不用点或者括号 调用不带任何参数的方法
    val str = "HelloWorld".toLowerCase
    
    
    
    //  == 已经被加工过了：首先检测左侧是否为null，如果不是，调用equals方法
    
    
    
    
    /*
                符号优先级
    * / %
    + -:
    = !
    < >
    &
    ^
    |
    
    */
    println(2<<2+2)  //32   <<起始于<，因此优先级比加法低，所以先执行+
    println(2+2<<2)    //16
    
    //但是有一个例外
    //如果操作符以等号字符=结束，且操作符并非比较操作符<=,>=,==或=
    //则这个操作符的优先级与赋值符=相同
    
    // x*=y+1 这个相同 x*=(y+1)
    
    //:字符结尾的方法由它的右手侧操作数调用，并传入右操作数，但无论怎样，操作数总是从左到右评估的
    // a:::b 变成 b.:::(a)
    // {val x=a;b.:::(a)}    a仍然在b之前被评估
    // a:::b:::c 被当做  a:::(b:::c)
    // a*b*c 被当做 (a*b)*c
     
    
    
    
    /**
     * 富操作
     */
    println( 0 max 3  )  //3
    println( 0.max(3)  )  //3
    
    println( -27.abs  )  //27
    println( -27 abs  )  //27
    
    println( -2.7 round )  //-3    java得到
    
    println( "bob" capitalize )  //Bob
    println("robert" drop 2 )  //bert
    
    
    
    
    
    
    
    
    
    
  }
}