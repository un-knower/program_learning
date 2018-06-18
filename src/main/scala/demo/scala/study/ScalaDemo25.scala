package demo.scala.study

object ScalaDemo25 {
  def main(args: Array[String]): Unit = {
      //方法 ,方法使用def进行定义
      //方法的返回值类型可以不写，编译器可以自动推断出来，但是递归必须制定返回类型
      // 方法名  参数列表                 返回值类型  方法体
      def m1(x:Int, y:Int):Int = x*y
      val result = m1(2,3)
      println(result)
      
      //函数，不能写返回值类型
      val f1 = (x:Int, y:Int) => x*y
      val res = f1(3,5)
      println(res)
      //函数可以作为一个值，传入到一个方法里面
      //如果对数组中的每个值加10或者*10
      //java需要写两个for循环，算法逻辑写在for循环中
      //scala可以定义两个函数，将函数传入
      val r = 1 to 10
      val func1 = (x:Int)=>x+10 
      val func2 = (x:Int)=>x*10
      r.map(func1)
      r.map(func2)
      //下面相当于传入了一个匿名函数
      r.map(_*10)
      r.map { (x:Int) => x*10 }
      r.map { x  => x*10 }
      
      
      print( mm(fu) ) //30
      
      
      //（高级）一个 函数，传入参数为一个Int，返回一个string
      //x代表传入的那个参数
      val start:Int => String = {x=>
          x.toString()
      }
      //匿名函数
      val start2 = (x:Int)=> x.toString()
      
      
      //下面两句是一样的
      val func3_1 = (x:Int, y:Double)=>(y,x)
      
      val func3_2:(Int,Double)=>(Double,Int) = {(a,b)=>
          (b,a)
      }
       
      
      //方法和函数的区别在于：函数可以作为参数出入方法中
      
      //方法可以通过神奇的 _ 转换为函数
      def method(x:Int,y:Int):Int = x+y
      val ff = (x:Int,y:Int) => x+y //如果一个函数与方法实现一样，重写一遍逻辑很傻
      val ff2 = method _   //可以通过神奇的_将方法改为函数
      
  }
  
  //方法中 传入一个函数，该函数有一个int类型的参数，返回一个int类型的值
  def mm(f:Int => Int):Int = {
      //在方法体里面调用函数
      f(3)
  }
  
  val fu=(x:Int)=>x*10
  
  
}