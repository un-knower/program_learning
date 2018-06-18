package demo.scala.study

object ScalaDemo3 {
   def main(args:Array[String]):Unit ={
     /**集合Set*/
     Set(1,2,3)
     
     /**元组Tuple
      * 元组是在不使用类的前提下，将元素组合起来形成简单的逻辑集合。
      * */
     val hostPort=("localhost",80)
     
     //与样本类不同，元组不能通过名称获取字段，而是使用位置下标来读取对象；而且这个下标基于1，而不是基于0
     println(hostPort._1)  //localhost
     println(hostPort._2)  //80
     
     hostPort match{
       case ("localhost",_)=>println("localhost1")
       case ("host",_)=>println("host1")
       case _=>println("_1")
     }//localhost1
     
     hostPort match{
     case ("localhost", port)=>println("localhost1 "+port)
     case ("host", port)=>println("host1 "+port)
     case _=>println("_1")
     }//localhost1 80
     
     //创建两个元素的元组时，可以使用特殊语法 ->
     println(1->2)  //(1,2)
     
     
     /**映射Map*/
     Map(1->2)
     Map("foo"->"bar")
     
     //Map()方法 使用变参列表
     //Map(1 -> "one", 2 -> "two")将变为 Map((1, "one"), (2, "two"))
     //映射的值可以是映射甚或是函数。
     //Map(1 -> Map("foo" -> "bar"))
     //Map("timesTwo" -> { timesTwo(_) })
     
     
     /**选项Option
      * Option 是一个表示有可能包含值的容器。

        Option基本的接口是这样的：
      * 
      * trait Option[T] {
           def isDefined: Boolean
           def get: T
           def getOrElse(t: T): T
        }
      * 
      * Option本身是泛型的，并且有两个子类： Some[T] 或 None
      * */
     
     //Map.get 使用 Option 作为其返回值，表示这个方法也许不会返回你请求的值。
     val numbers = Map("one"->1,"two"->2,"three"->"3")
     println(numbers.get("one"))  //Some(1)
     println(numbers.get("three"))  //Some(3)
     println(numbers.get("four"))  //None
     
     //建议使用getOrElse或模式匹配处理这个结果,从Option中提取数据
     val result = numbers.get("one").getOrElse(0) //与下面一句作用一致
     //val result = numbers.getOrElse("one", 0);
     println(result)  //1
     
     //模式匹配能自然地配合Option使用
     val result2 = numbers.get("one") match {
       case Some(n)=>n
       case None=>0
     }
     println(result2) //1
     
     
     
     /**函数组合子（Functional Combinators）*/
     /**
      * List(1, 2, 3) map squared
      * 对列表中的每一个元素都应用了squared平方函数，并返回一个新的列表List(1, 4, 9)。我们称这个操作map 组合子。
      */
     
     var numbersList= List(1,2,3)
     println(numbersList.map(i=>i*2))  //List(2, 4, 6)
     
     //或传入一个部分应用函数
     def timesTwo(i:Int):Int = i*2
  
     println(numbersList.map {timesTwo })  //List(2, 4, 6)
     println(numbersList.map(_*2) )  //List(2, 4, 6)
     
     /**foreach
      * 没有返回值
      * */
     println(numbersList.foreach { i => i*2 }) //()
     
     /**filter
      * filter移除任何对传入函数计算结果为false的元素。
      * 保留满足条件的数据，即true
      * 返回一个布尔值的函数通常被称为谓词函数[或判定函数]
      * */
     println(numbersList.filter { x => x%2==0 })  //List(2)
     
     println(numbersList.filter { _%2==0})  //List(2)
     /**zip
      * 将两个列表的内容聚合到一个对偶列表中
      * */
     val li=List(1,2,3).zip(List("a","b","c"))
     println(li)  //List((1,a), (2,b), (3,c))
     val li2=List(1,2,3,4).zip(List("a","b","c"))
     println(li2) //List((1,a), (2,b), (3,c))
     val li3=List(1,2,3).zip(List("a","b","c","d"))
     println(li3) //List((1,a), (2,b), (3,c))
     
     
     /**partiton
      * 将使用给定的谓词函数分割列表
      * */
     val numbersP = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
     println(numbersP.partition( _ %2 == 0))  //(List(2, 4, 6, 8, 10),List(1, 3, 5, 7, 9))
     println(numbersP.partition( _ %2 == 1))  //(List(1, 3, 5, 7, 9),List(2, 4, 6, 8, 10))
     
     val strP = "Hello World"
     println(strP.partition { x => x.isUpper }) //(HW,ello orld)
     
     
     /**find
      * 返回集合中第一个匹配谓词函数的元素
      */
      println(numbersP.find(i=>i>5))  //Some(6)
      
     /**
      * drop & dropWhile
      * 
      */
     
     //drop 将删除前i个元素
      println(numbersP.drop(6))  //List(7, 8, 9, 10)
      println(numbersP)  //List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
     
     //dropWhile 将删除元素直到找到第一个匹配谓词函数的元素
      
      println(numbersP.dropWhile { i => i%2!=0 })  //List(2, 3, 4, 5, 6, 7, 8, 9, 10)
     
      
      
      /**
       * foldLeft
       * 
       */
                              //m作为累加器，将0初始化m
      println(  numbersP.foldLeft(0)((m,n)=>m+n)  )   //55
                              //n作为累加器，将0初始化n
      println(  numbersP.foldRight(0)((m,n)=>m+n)  )   //55
      
      println(numbersP.sum)  //55
      
      /**
       * flatten
       * 将嵌套结构扁平化一个层次的集合
       */
      println( List(List(1,2,3),List(4,5)).flatten )  //List(1, 2, 3, 4, 5)
    
      /**
       * flatMap
       * flatMap是一种常用的组合子，结合映射[mapping]和扁平化[flattening]。 flatMap需要一个处理嵌套列表的函数，然后将结果串连起来。
       */
      val nestedNumbers=List(List(1,2,3),List(4,5))
      println(nestedNumbers.flatMap { x => x.map { i => i*2 } })  //List(2, 4, 6, 8, 10)
      
      
      /**
       * 
       */
      
      println(numbersP) //List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      def ourMap(numbers:List[Int], fn:Int=>Int):List[Int]={
       numbers.foldRight(List[Int]()){
         (x:Int,xs:List[Int])=> fn(x)::xs
       }
     }
     println( ourMap(numbersP,timesTwo(_)) ) //List(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
      
      
     /**Map
      * 所有展示的函数组合子都可以在Map上使用。Map可以被看作是一个二元组的列表，所以你写的函数要处理一个键和值的二元组。
      */
     
     val extensions = Map("steve"->100,"bob"->101,"joe"->201)
     //筛选第二项>200的
     println( extensions.filter((namePhone:(String,Int))=>namePhone._2>200) )  //Map(joe -> 201)
     println( extensions.filter(_._2>200 ))  //Map(joe -> 201)
     
     
     
     
      
   }
}