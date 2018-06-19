package demo.scala.study

/**
 * @author qingjian
 */
//被包在花括号内 没有 match的一组case语句是一个偏函数
//它是PartialFunction[A,B]的一个实例，A代表参数类型，B代表返回类型
//常用作输入模式匹配
object PartialFunction {
    //                          输入     , 返回
    def func1:PartialFunction[String, Int] = {
        case "one" => 1
        case "two" => 2
        case _ => -1
    }
    //与上面偏函数的功能的一样的
    def func2(num :String):Int = num match {
        case "one" => 1
        case "two" => 2
        case _ => -1
    }
    
    def main(args: Array[String]):Unit= {
        println(func1("one"))
        println(func2("two"))
    }
}