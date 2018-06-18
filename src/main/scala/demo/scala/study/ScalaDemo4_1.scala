package demo.scala.study

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
 * @author qingjian
 */
object ScalaDemo4_1 {
    def main(args: Array[String]): Unit = {
   		val myCounter = new Counter  //或 new Counter()
		myCounter.increment()
		println(myCounter.current())    //1 或者调用无参函数 myCounter.current
      
        //Person类
        var p = new Person
        println(p.age)  //get  //0
        p.age = 3  //set
        println(p.age)    //3
       
        var p2=new Person2
        println(p2.age)    //20
        p2.age(30)
        p2.age(10)
        println(p2.age)    //30
        
        
        var p3 = new Person3
        println(p3.name)    //null
        p3.name="lilei"
        println(p3.name)    //lilei
        p3.setName("lilei2")
        println(p3.getName())    //lilei2
        
        
        val p4 = new Person4()  //主构造器
        val p4_1 = new Person4("Fred") //第一辅助构造器
   		val p4_2 = new Person4("Fred",3) //第二辅助构造器
        
        val p5 = new Person5("John",4)
        println(p5.name)    //John
            
        val p6= new Person6("h",5)    //hello
        

        
        /**
         *嵌套类
         *  
         */
        //有如下两个网络
        val chatter = new Network
        val myFace = new Network        
        val fred = chatter.join("Fred")   //fred属于chatter网            //val fred: chatter.Member
        val wilma = chatter.join("Wilma") ////wilma属于chatter网    //val wilma: chatter.Member
        fred.contacts+=wilma  //ok
        val barney = myFace.join("Barney")  //barney属于myFace网        //val barney: myFace.Member
        //fred.contacts+=barney  //报错 因为外部类不同，与java不同
        
        
        //////////////////////
        //有如下两个网络
        val chatter2 =  Network2  //会调用apply方法
        val myFace2 =  Network2        
        val fred2 = chatter2.join("Fred")   //fred属于chatter网                //val fred2: chatter2.Member2
        val wilma2 = chatter2.join("Wilma") ////wilma属于chatter网        //val wilma2: chatter2.Member2
        fred2.contacts+=wilma2  //ok
        val barney2 = myFace2.join("Barney")  //barney属于myFace网        //val barney2: myFace2.Member2
        fred2.contacts+=barney2  //没有报错，是半生类，脱离了class嵌套class
        
        
        //
        val r = new RequireClass(1,2)
        println(  r.toString()  )
        
        //val r1 = new RequireClass(1,0)
   		//println(  r1.toString()  )  //报错
        
        
        
    }
}

/**
 * 类
 * 类并不声明为public，Scala源文件中可以包含多个类，所有这些类都具有公有可见性
 */
class Counter {
    private var value=0    //必须初始化字段
    def increment() {  //过程
        value+=1
    }
    def current()=value  //函数 或无参函数 def current=value
}


/**
 * getter/setter
 * scala对每个字段都提供getter/setter方法，
 * scala生成面向jvm的类，其中如果一个字段是私有的，那么其对应的getter和setter方法都是私有的
 * 其中如果一个字段是共有的，那么其对应的getter和setter方法也是公有的
 * 在scala中，getter和setter分别叫做   字段名   和  字段名_
 */
class Person{
    var age=0  //var声明会生成getter和setter方法
    val name="lili"  //val声明只会生成getter方法
}

//age的getter和setter方法分别叫做  age 和  age_

class Person2 {
    private var privateAge=20
    //重构getter setter
    def age=privateAge
    def age(newValue:Int) {
        if(newValue>privateAge) privateAge=newValue
    }
    
}
/**
 * 标注 setter/getter
 */
class Person3 {
    /**
     * 将scala字段标注为@BeanProperty时，会自动生成四个方法：
     * 1.name:String
     * 2.name_=(newValue:String):Unit
     * 3.getName():String
     * 4.setName(newValue:String):Unit
     */
    @BeanProperty var name:String=_   
}


/**
 * 辅构造器
 * this
 * 
 */
class Person4 {
    private var name=""
    private var age=0
    
    def this(name:String) { //一个辅助构造器
        this() //调用主构造器
        this.name=name
    }
    def this(name:String,age:Int) { //另一个辅构造器
        this(name)
        this.age=age
    }
    
    
    
}

/*
 * 主构造器
 * 每个类都有一个主构造器，与类定义交织在一起
 */

class Person5(val name:String,val age:Int) {
    //这里的内容就是主构造器的参数
    
}

//下例子中println会在对象构造出来的时候执行
class Person6(val name:String,val age:Int) {
    println("hello")
}


/**
 * 
 * 嵌套类
 * 
 * 下面嵌套类不同的社交网络的成员，不同相互添加
*/

class Network {
    class Member(val name:String) {
        val contacts = new ArrayBuffer[Member]
    }
    private val members= new ArrayBuffer[Member]
    def join(name:String)= {
        val m=new Member(name)
        members+=m
        m
    }
}

//如果希望可以相互添加,可以将Member类移到别处，比如Network2的伴生对象

object Network2 {
    class Member2(val name:String) {
        val contacts = new ArrayBuffer[Member2]
    } 
    private val members= new ArrayBuffer[Member2]
    def join(name:String)= {
        val m=new Member2(name)
        members+=m
        m
    }
    
}
//或者使用 类型投影  Network#Member  其含义是任何Network的Member
//val contacts = new ArrayBuffer[Network#Member]


/*
 * 为主构造器定义先决条件
 * require方法，如果出入的值为真，则require将正常返回
 * 反之，require则通过抛出 IllegalArgumentException来阻止对象被创建
 */
class RequireClass(n:Int, d:Int) {
    require(d!=0)
    override def toString = n+"/"+d
    
}
