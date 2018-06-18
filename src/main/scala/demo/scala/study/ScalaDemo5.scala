package demo.scala.study

/**
 * 继承
 */


class Person11{
  var name=""  //必须初始化
  override def toString=getClass.getName+"[name="+name+"]" //重写方法，必须加上 override
}

class Employee extends Person11{  //继承用 extends
  
}

/**
 * isInstanceOf[] 判断某个对象是否属于某个给定的类
 * 如果p指向的是Employee类及其子类（比如Manager）的对象，则p.isInstanceOf[Employee]将会成功
 * 如果是，就可以用asInstanceOf[] 将引用转换为子类的引用
 * if(p.isInstanceOf[Employee]) {
 *  val s = p.asInstanceOf[Employee]
 * 
 * }
 * 
 * 
 * 如果想判断p指向的是一个Employee对象，但又不是其子类的话，可用
 * if (p.getClass == classOf[Employee])
 * {}
 * 
 * 不过，与类型检查和转换相比，模式匹配通常是更好的选择
 * p match {
 *  case s:Employee=> ..
 *  case _=>
 * }
 * 

 * 
 */



/**
  * 匿名子类
 * val alien = new Person("Fred") {
 *  def greeting="Greetings,Earthing!"
 * }
 * 这将创建出一个结构类型的对象，该类型记作 Person{def greeting:String}
 * 可以用这个类型作为参数类型的定义
 * def meeting(p:Person{def greeting:String}) {
 *  println(p.name+"says: "+p.greeting)
 * }
 */


/**
 * 抽象字段
 * 
 */

abstract class PersonAbstract{
  val id:Int   //没有初始化，这是一个带有抽象的getter方法
  var name:String  //另一个抽象字段，带有抽象的getter和setter方法
  def abs()
}

//但是具体的子类必须提供具体的字段

class EmployeeImp(val id:Int) extends PersonAbstract {//子类有具体的id属性
  var name=""  //和具体的name属性
  def abs() {
      
  }
}
//和方法一样，在子类中重写超类中的抽象字段时，不需要override关键字
//可以随时用匿名类型来定制抽象字段


/**
 * range的值在超类的构造器中用到了，而超类的构造器先于子类的构造器运行
 * 1.Ant的构造器在做它自己的构造器之前，调用Creature的构造器
 * 2.Creature的构造器将它的range字段设为10
 * 3.Creature的构造器为了初始化env数组，调用range()取值器。！！！！！！
 * 4.该方法被重写以输出（还未初始化的）Ant类的range字段值
 * 5.range返回0
 * 6.env被设为长度为0的数组
 * 7.Ant构造器继续执行，将其range字段设为2
 * 
 */
class Creature {
  val range:Int = 10
  val env:Array[Int]=new Array[Int](range) //这里看起来是个字段，其实会调用range的getter方法
}
class Ant extends Creature {
  override val range=2
}
object ScalaDemo5 {
  def main(args: Array[String]): Unit = {
    var ant = new Ant()
    println(ant.env.length) //0
    
  }
}

/**上面的问题有如下几种解决方法：
 * (1)将 val 声明为final 。这样很安全，但并不灵活
 * (2)在超类中将val声明为lazy。这样很安全但不高效
 * (3)在子类中提前定义语法:
 *  class Ant extends {
 *    override val range = 2
 *  } with Creature
 *  
 */

/**
 * 继承层级
 * 与java中节本类型相对应的类，以及Unit类型，都拓展自AnyVal
 * 所有其他类都是AnyRef的子类。AnyRef是java或。net虚拟机中Object类的同义词
 * 
 */

/**
 * 对象相等性
 * AnyRef 的eq方法检查两个引用是否指向同一个对象，AnyRef的equals方法调用eq
 * 所以如果你认为如果两个物件有着相同的描述和价格的时候他们就是相等的，需要重写equals方法
 * final override def equals(other:Any)= {
    val that = other.asInstanceOf[Item]
    if (that == null) false
    else description==that.description && price == that.price
   }
 */
