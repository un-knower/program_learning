package demo.scala.study

object ScalaDemo2 {
  def main(args:Array[String]):Unit ={
    
    /**apply 方法
     * object-对象加括号相当于调用对象的apply方法
     * */
    //当类或对象有一个主要用途的时候，apply方法为你提供了一个很好的语法糖。
    class Foo{
      println("ss")
    }
    object FooMaker{
      def apply()=new Foo    //ss
    //def apply()=new Foo()
      
    }
    val newFoo = FooMaker()
    
    //或者
    class Bar{
      def apply()=1
    }
    val bar = new Bar()
    println(bar()) //1
    
    
    /**单例对象 
     * 用于持有一个类的唯一实例
     * 通常用于工厂模式
     * */
   object Timer{
      
      var count=0;
      def currentCount():Long ={
        count+=1
        count
      }
    } 
    
    println(Timer.currentCount())  //1
    println(Timer.currentCount())  //2
    println(Timer.currentCount())  //3
    
    
    //单例对象可以和类具有相同的名称，此时该对象也被称为“伴生对象”。我们通常将伴生对象作为工厂使用。
    //可以不需要使用new来创建一个实例了
    class Bar2(foo:String)
    object Bar2{
      def apply(foo:String)=new Bar2(foo)
    }
    
    
    /**函数即对象
     * Scala拥有第一类函数：first-class function。
     * 不仅可以定义函数和调用它们，还可以把函数写成没有名字的文本：literal并把它们像值：value那样传递
     * 函数文本被编译进一个类，类在运行期实例化的时候是一个函数值：function value。
     * 任何函数值都是某个扩展了若干scala包的FunctionN特质之一的类的实例，如Function0是没有参数的函数，
     * Function1是有一个参数的函数等等。每个FunctionN特质有一个apply方法用来调用函数。
     * 
     * 
     * */
    //函数是一些特质的集合。具体来说，具有一个参数的函数是Function1特质的一个实例。
    //这个特征定义了apply()语法糖，让你调用一个对象时就像你在调用一个函数。
    //这个Function特质集合下标从0开始一直到22
    //
    object addOne extends Function1[Int,Int] {
      def apply(m:Int):Int=m+1
    }
    println(addOne(1))  //2
    
    
    //类也可以扩展Function，这些类的实例可以使用()调用
    class AddOne extends Function1[Int,Int] {
      def apply(m:Int):Int=m+1
    }
    val plusOne = new AddOne()
    println(plusOne(1))  //2
    
    
    //可以使用更直观快捷的extends (Int => Int)代替extends Function1[Int, Int]
    
    class AddOne2 extends (Int=>Int) {
      def apply(m:Int):Int=m+1
    }
    
    class AddOne3 {
        def apply(m:Int):Int=m+1
    } 
    val AddOne3 = new AddOne3()
    println(AddOne3(2))  //3
    
    
    
    /**
     * (x: Int) => x + 1 
     * =>指明这个函数把左边的东西（任何整数x）转变成右边的东西（x + 1）。所以，这是一个把任何整数x映射为x + 1的函数。
     * 
     */
    
    var increase = (x:Int)=>x+1   //var increase: Int => Int
    println(increase(10))  //11
    
    var increaseS =(x:String,y:Int)=>  //var increaseS: (String, Int) => Int
    {
        1
    }
    
    
    val someNumbers=List(-1,2,3,4,5,6,7)
    someNumbers.foreach { x => println(x) }
    println("---------------------")
    someNumbers.filter { x => x>0 }.foreach { x => println(x) }
    
    
    /**
     * the type Int => String, is equivalent to the type Function1[Int,String] i.e. a function that takes an argument of type Int and returns a String.
     * 
     * () => T is the type of a function that takes no arguments and returns a T. It is equivalent to Function0[T]. 
     * () is called a zero parameter list I believe.
     */
    
    

    
    
    /**模式匹配*/
    val times=1
    times match{
      case 1=>"one"
      case 2=>"two"
      case _=>"some other number"
    }
    
    //使用守卫来进行匹配
    val times2=2
    val t=times2 match{
      case i if i==1 =>1
      case i if i==2 =>"two"
      case _  =>"some other number"
    }
    println(t) //two
   
    /**
     * 样本类
     * Case Classes
     */
    //使用样本类可以方便得存储和匹配类的内容。你不用new关键字就可以创建它们。
    case class Calculator(brand:String,model:String)
    val hp20b=Calculator("hp","20b")
    
    
    /*
    try{
      
    }catch{
    }finally{
      
    }
    */
    
    
    
    
    
  }//main
  
  
  
  
  
  
  
  
  
}