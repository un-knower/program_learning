package demo.scala.study

import scala.util.Random


//模式匹配
object ScalaDemo_match extends App{
    //匹配字符串内容
    println("匹配字符串内容")
    val arr = Array("abc","def","a")
    val name = arr(Random.nextInt(arr.length))
    
    name match {
        case "abc" => {println("abc")}
        case "def" => println("def")
        case "a" => println("a")
        case _ => println("none")
    }
    
    
    //匹配类型
    println("匹配类型")
    val arr2 = Array("hello",1,-2.0, ScalaDemo_match)
    val elem = arr2(Random.nextInt(arr2.length))
    
    elem match {
        case x:Int => println("Int "+x)
        case y:Double if(y >= 0) => println("Double "+y)
        case z:String => println("String "+z)
        case _ => throw new Exception("not match exception")
        
    }
    
    //匹配array里面的内容
    println("匹配array里面的内容 1")
    //val arr3 = Array(0, 1, 5)
    val arr3 = Array(1, 1, 5)
    arr3 match {
        case Array(1, x, y) => println(x +" "+ y)
        case Array(0, 1, 5) => println("only 0")
        case Array(0, _*) => println("0,...")
        case _ => println("something else")
        
    }
    
    
    println("匹配array里面的内容 2")
    val lst = List(0, 3, 111)
    lst match {
        case 0 :: Nil => println("only 0")
        case x :: y :: Nil => println(s"x: $x y: $y") //匹配两个元素
        case 0 :: a => println(s"0 ...$a") //进入这里了
        case _ => println("something else")
    }
    
    println("匹配tuple里面的内容")
    val tup = (1,2,3)
    tup match {
        case (1, x, y) => println(s"x=$x, y=$y")
        case (_, z, 5) => println(z)
        case _ => println("else")
    }
    
    
    
    
}