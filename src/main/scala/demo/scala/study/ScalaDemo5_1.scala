package demo.scala.study

/**
 * @author qingjian
 */
object ScalaDemo5_1 {
  def main(args: Array[String]): Unit = {
    /**
     * 对象
     * Scala没有静态方法和静态字段，可以使用object这个语法结构来达到同样的目的。
     * 对象定义了某个类的单个实例
     * 在java或者c++会使用单例对象的地方，在scala都可以用对象来实现
     * 1.作为存放工具函数或常量的地方
     * 2.高效的共享单个不可变实例
     * 3.需要用单个实例来协调某个服务时（参考单例模式）
     */
      println(Accounts.newUniqueNumber())    //1
      println(Accounts.newUniqueNumber())    //2
      println(Accounts.newUniqueNumber())    //3
     
      
      /**
       * 伴生对象
       * 在java或c++中，你通常会用到既有实例方法又有静态方法的类
       * 在scala中，你可以通过类和与类同名的“伴生”对象来达到同样的目的
       * 类和它的伴生对象可以相互访问私有特性。他们必须存在于一个源文件中。
       */
      
      /**
       * apply方法
       * 我们通常会定义和使用对象的apply方法，当遇到如下形式的表达式时，apply方法就会被调用
       * Object (参数1,...,参数n)
       * 通常一个apply方法返回的是伴生类的对象
       */
      //Array对象定义了apply方法
      //为什么不用构造器，对于嵌套表达式来讲，省去new关键字会方便很多。
      //Array(100) 和  new Array(100) 很容易搞混。前一个表达式调用的是apply(100)，输出一个单元素（整数100）的Array[Int]
      //而后一个表达式调用的是构造器this(100)，结果是Array[Nothing]，包含了100个null元素
      /**
       * scala> Array(10)
         res0: Array[Int] = Array(10)

         scala> new Array(10)
         res1: Array[Nothing] = Array(null, null, null, null, null, null, null, null, null, null)
       */
      Array("Mary","had","a","little","lamb")
      
      /**
       * 
       */
  }
}




object Accounts {
    private var lastNumber =0
    def newUniqueNumber()={
        lastNumber+=1
        lastNumber
    }
}