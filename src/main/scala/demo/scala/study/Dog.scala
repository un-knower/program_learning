package demo.scala.study

class Dog(age:Int) {

}

object Dog {
    def apply() = {
        println("apply invoked")
    }
    
    def apply(name:String) = {
        println(name)
    }
    
    def apply(age:Int):Dog = { //返回对象
        new Dog(age)
    }
    def main(args: Array[String]): Unit = {
        val d = Dog //没有任何打印 //object Dog的静态对象
        val d0 = Dog()  //apply invoked  //没有new会到object中找参数一致的apply方法
        
        val d2 = Dog("dog") //dog
        
        var arr = Array(1,2,3,4)  //调用Array的apply方法
        
        
        val d3 = Dog(123)
        
    }
}