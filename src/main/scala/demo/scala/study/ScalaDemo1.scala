
package demo.scala.study

import java.io.IOException

object ScalaType {
  def main(args: Array[String]): Unit = {
      
      
    for(i<- Range(10,0,-1)) {
        println(i) // 10 9 8 7 6 5 4 3 2 1 不包含end 
    }  
    /**常量、变量*/
    //val 定义常量1
    val msg="Hello World"
    println(msg)
    //msg=""; 这里报错
    
    //定义常量2
    val msg2:String="Hello Again"
    
    //定义变量
    var canChange="1234";
    canChange="222"
    
    var canChange2:String="33333"
    
    /*函数*/
    //def 定义函数
    //函数的返回结果无需使用return语句。函数的最后一个表达式的值就可以作为函数的结果作为返回值
    def max(x:Int,y:Int):Int={
      if(x>y) x
      else y
    }
    
    println(max(2,3)) //3
    
    //为函数参数指定类型签名
    def addOne(m:Int):Int=m+1
    val three=addOne(2)
    println(three) //3
    
    //如果函数不带参数，可以不写参数
    def f()=1+2
    println(f()) //3

    println(f) //3
    
    def ff=1+100
    println(ff) //101
    
    //匿名函数
    val addTwo=(x:Int)=>x+2
    println(addTwo(2))  //4
    
    //如果你的函数有很多表达式，可以使用{}来格式化代码，使之易读。
    def timesTwo(i:Int):Int={
      println("Hello")
      i*2
    }
    println(timesTwo(3)) //Hello
                         //6
    
    
    /***
     * Scala对于不返回值的函数有特殊的表示方法。
     * 如果函数体包含在花括号当中，但没有前面的=号，那么返回类型就是Unit。这样的函数被称作过程(procedure)
     * 过程不返回值，我们调用它仅仅是为了它的副作用。
     * 
     */
    
    
    /**部分应用*/
    //使用下划线“_”部分应用一个函数，结果将得到另一个函数,部分应用参数列表中的任意参数，而不仅仅是最后一个。
    def adder(m:Int,n:Int)=m+n
    val add2=adder(2,_:Int)
    println(add2(3)) //5
    
    
    
    /**柯里化函数
     * 柯里化是把接受多个参数的函数变换成接受一个单一参数（最初函数的第一个参数）的函数，并且返回接受余下的参数而且返回结果的新函数的技术。 
     * */
    //有时会有这样的需求：允许别人一会在你的函数上应用一些参数，然后又应用另外的一些参数。
    def multiply(m:Int)(n:Int):Int=m*n
    println(multiply(2)(3))  //6
    //填上第一个参数并且部分应用第二个参数
    val timesTwo2=multiply(2)_
    println(timesTwo2(3))  //6
    
    
   //可以对任何多参数函数执行柯里化
    (adder _).curried   //(Int) => (Int) => Int = <function1>
    
    /**可变长度参数*/
    def capitalizeAll(args:String*)={
      args.map(arg=>arg.capitalize)
    }
    println(capitalizeAll("rarity","applejack"))  //ArrayBuffer(Rarity, Applejack)
    
    
    /**类**/
    class Calculator{
      val brand:String="Hi"
      def add(m:Int,n:Int):Int=m+n
    }
    
    val calc = new Calculator
    println(calc)  //ScalaType$Calculator$1@a3a380
    
    println(calc.add(1, 2)) //3
    println(calc.brand)  //Hi
    
    
    //构造函数
    class Calculator2(brand:String) {
      /**
       * A constructor
       */
      val color:String=if(brand=="TI") {
        "blue"
      }else if(brand=="HP") {
        "black"
      }else {
        "white"
      }
       def add(m: Int, n: Int): Int = m + n
    }
    
    val calc2 = new Calculator2("HP")
    println(calc2.color)  //black
    
    
    
    
    
    
    
    //继承
    // Effective Scala 指出如果子类与父类实际上没有区别，类型别名是优于继承的
    class ScientCalculator(brand:String) extends Calculator2(brand) {
      def log(m:Double,base:Double)=math.log(m)/math.log(base)
    } 
    
    //抽象类
    //抽象类定义了一些方法但没有实现它们。取而代之是由扩展抽象类的子类定义这些方法。你不能创建抽象类的实例。
    
    abstract class Shape{
      def getArea():Int
    }
    class Circle(r:Int) extends Shape {
      def getArea():Int={
        r*r*3
      }
    }
    
    //val s = new Shape() //报错  error: class Shape is abstract; cannot be instantiated
    val c = new Circle(2)
    println(c.getArea());  //12
    
    
    
    /**特质
     * 特质是一些字段和行为的集合，可以扩展或混入（mixin）你的类中
     * 意义与java的接口类似，可以在trait中可以实现部分方法
     * */
    trait Car{
      val brand:String
    }
    trait Shiny{
      val shineRefraction:Int
    }
    //通过with关键字，一个类可以拓展多个特质
    class BMW extends Car with Shiny {
      val brand="BMW"
      val shineRefraction=12
    }
    
    
    /**泛型*/
    //函数也可以是泛型的，来适用于所有类型
    //用 方括号 语法引入的类型参数
    trait Cache[K,V] {
      def get(key:K):V
      def put(key:K,value:V)
      def delete(key:K)
      def remove[K](key:K)
    }
    
    
    
    /**
     * 格式输出，输入
     * println
     * print
     * printf
     * readLine
     * readInt
     */
   // val name=readLine("Your name:")
    println("Your age:")
    //val age=readInt()
    ///printf("Hello,%s! Next year,you will be %d\n",name,age+1)
    
    /**
     * 循环
     */
    val s="Hello"
    var sum=0
    for(i<- 0 until s.length) {
      sum+=i
    }
      
    println(sum)  //10
    sum=0
    for(ch<-"Hello") {
      sum+=ch  //72+101+108+108+111
    }
    println(sum)  //500
    
    
    //两个for循环
    for(i<- 1 to 3;j<- 1 to 3){
      print((10*i+j)+" ")  //11 12 13 21 22 23 31 32 33
    }
    println()
    
    //for生成器可以带一个守卫
    for(i<- 1 to 3;j<- 1 to 3 if i!=j)
      print((10*i+j)+" ")    //12 13 21 23 31 32 
    
      
    println()
    //可以任意多定义
    //3重for循环
     for(i<- 1 to 3;from=4-i;j<- from to 3) {
       print((10*i+j)+" ")  //13 22 23 31 32 33 
       
     }
      
     //for循环体以yield开始
    for(i <- 1 to 10) yield i%3  //生成 Vector(1,2,0,1,2,0,1,2,0,1) 
     
    
    /**
     * 函数
     * def funname() = {
     * }
     * 并不需要return，可以把return当做函数版的break
     */
    def abs(x:Double)=if (x>=0)x else -x
    
    //默认参数和带名参数
    def decoreate(str:String,left:String="[",right:String="]")= {
      left+str+right
    }
    println(decoreate("hello", "<<", ">>"))  //<<hello>>
    println(decoreate("hello"))  //[hello]
    println(decoreate("hello",">>"))  //>>hello]
    //
    println(decoreate(left="<<<",str="hello",right=">>>"))  //<<<hello>>>
    
    //变长参数
    //函数得到的是一个类型为Seq的参数
    def sumAll(args:Int*)= {
      var result=0
      for (arg<-args) 
        result+=arg
      result
    }
    
    println(sumAll(1,2,3,4))  //10
    println(sumAll(1,2,3,4,5))  //15
    
    //sumAll(1 to 5)错误 因为该函数的参数必须是单个整数，而不是一个整数区间
    val sumall_re=sumAll(1 to 5:_*)  //_* 会告诉编译器这个参数被当做参数序列处理
    println(sumall_re)  //15
    
    
    /**
     * 过程
     * 是scala对于不返回值的函数的特殊表示法
     * 如果函数体包含在花括号当中，但没有前面的=号，那么返回类型就是Unit。
     * 这样的函数被称作过程。过程不返回值
     * 
     */
    def box(s:String) {
      println(s)
    }
   
    /**
     * 懒值
     * 当val被声明为lazy时，他的初始化将被推迟，直到我们首次对它取值
     */
    lazy val word=scala.io.Source.fromFile("/usr/share/dict/words").mkString
    //初始化语句被执行时不会报错，但一旦访问words时，如果没有找到文件，就会报错
    
    
    /**
     * 异常
     * 与java不同的是，scala没有“受检”异常，不需要声明说函数或方法可能会抛出某种异常
     */
    val x=10
    if (x>=0) {
      //sqrt(x)
    }
    else {
      throw new IllegalArgumentException("x should not be negative")
    }
    
    //try{...} catch{...} finally{...}
    try {
      
    }catch {
      case _:IOException=>println("bad io")
      case ex:Exception=>println("exception")
    } finally {
        //...
    }
    
    
    
    
    
    
    //定义文件名
    //方法1
    var filename = "default.txt"
    if(!args.isEmpty)
        filename = args(0)
    //方法2 
    var filename2 = if(!args.isEmpty) args(0) else "default.txt"
    
    
    
    
    
  }
}