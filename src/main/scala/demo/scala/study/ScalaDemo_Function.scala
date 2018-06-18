package demo.scala.study

/**
 * @author qingjian
 * 函数说明
 */
/**
 * 减少代码重复：高阶函数
 * 高阶函数：带其他函数作为参数的函数
 */
//例如以下三个函数
object FileMatcher {
    private def filesHere = (new java.io.File(".")).listFiles
    //函数1，搜索所有拓展名为.scala的文件
    def filesEnding(query: String) =
        for (file <- filesHere; if file.getName.endsWith(query))
            yield file

    //函数2，搜索存在query字符的文件
    def filesContaining(query: String) =
        for (file <- filesHere; if file.getName.contains(query))
            yield file
    //函数3，搜索满足正则表达式的文件
    def filesRgex(query: String) =
        for (file <- filesHere; if file.getName.matches(query))
            yield file

    //上面三个函数只有文件的调用方法不同，因此我们更希望构造如下的函数
    //def filesMatching(query:String, method)=
    //    for(file<-filesHere;if file.getName.method(query))
    //这种方法在某些动态语言中能其作用，但scala不允许在运行期这样粘合代码

    def filesMatching(query: String,
                      matcher: (String, String) => Boolean) = {
        for (file <- filesHere; if matcher(file.getName, query))
            yield file
    }

    //其中matcher就是一个函数，这个函数类型是(String,String)=>Boolean
    //有了这个方法，可以通过三个搜索方法调用它
    def filesEnding2(query: String) = filesMatching(query, _.endsWith(_));
    def filesContaining2(query: String) = filesMatching(query, _.contains(_));
    def filesRgex2(query: String) = filesMatching(query, _.matches(_))

    //还可以更精简
    def filesMatching(matcher: String => Boolean) = {
        for (file <- filesHere; if matcher(file.getName))
            yield file
    }

    def filesEnding3(query: String) = filesMatching(_.endsWith(query))
    def filesContaining3(query: String) = filesMatching(_.contains(query))
    def filesRgex3(query: String) = filesMatching(_.matches(query))

}
object ScalaDemo_Function {
    def main(args: Array[String]): Unit = {

        /**
         * 闭包
         */
        //(x:Int) => x+more
        //上句报错，因为 变量more是一个自由变量，函数文本本身没有给出其含义
        //相对来说，x是一个绑定变量，因为它在函数的上下文中有明确意义

        //另一方面，只要有一个叫做more的什么东西同样的函数文本将工作正常
        var more = 1
        val addMore = (x: Int) => x + more
        //依照这个函数文本在运行时创建的函数值（对象）被称为 闭包 closure
        //名称源自于通过“捕获”自由变量的绑定对象对函数文本执行的关闭行动。

        /*
       * 
       * 重复参数
       */
        def echo(args: String*) = {
            //for(arg<-args) println(arg)
            args.foreach { println }
        }

        def sum(n: Int, r: Int) = {
            var rr = r
            for (i <- 1 to 10) {

                rr += i

            }
            rr
        }

        println(sum(1, 1))

    }

}