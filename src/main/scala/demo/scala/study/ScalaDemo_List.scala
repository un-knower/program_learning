package demo.scala.study

import scala.util.Random

/**
 * @author qingjian
 */
object ScalaDemo_List {
    def main(args: Array[String]): Unit = {
      val arr = new Array[Integer](3) //表示的是数组长度为3的Integer类型数组
      val arr_ = Array[Integer](3)  //表示的是数组元素为3的长度为一的数组
        
        
      //空List
      val emptyList1 = List()
      val emptyList2 = Nil;
      
      //创建新的List
      val list1 = List("Cool","tools","rule")
      val list2 = "Will"::"fill"::"until"::Nil   //最后要加一个Nil，否则会导致编译失败
      
      
      
      
      
      
      //叠加两个List
      List("a","b"):::List("c","d")
      
      List(1,2,4,5):::6::Nil
      
      
      //取值
      println( list1(2) )  //rule
      
      println( list1.count { x => x.length()==4 } )   //2
      println( list1.filter(_.length()==4) )   //List(Cool, rule)

      //删除
      val list1_drop = list1.drop(2)   //删除前两个元素
      println(list1_drop)   //List(rule)
      
      
      val list2_dropRight = list1.dropRight(2)
      println( list2_dropRight ) //List(Cool)
      
      
      println(list1.filterNot(s=> s.length ==4))  //List(tools)   //remove函数报错
      
      
      
      //判断元素是否存在
      println( list1.exists { x => x=="until" } )  //false
      println( list1.exists { x => x=="tools" } )  //true
      
      //遍历
      //对列表中每个字符串执行
      println( list1.foreach { x => println(x) })
      println( list1.foreach {  println })
        //Cool
        //tools
        //rule
        //()
      
      println(list1.forall { x => x.endsWith("l") })  //false  判断列表中所有元素是否满足
      
      println( list1.length )   //3
      
      println(list1.head)   //Cool //返回第一个元素
      println(list1.last)  //rule  //返回最后一个元素
      println(list1.tail)    //List(tools, rule) //返回除了第一个元素之外的元素list
      println(list1.init)  //List(Cool, tools)  //返回除了最后一个元素之外的元素list
      
      list1.isEmpty  //判断是否为空
      
      //map
      println(list1.map { x => x+"y" })   //List(Cooly, toolsy, ruley)
      
      println( list1.mkString(",") )   //Cool,tools,rule
      
      //flatMap
      
      def ulcase(s:String) = Vector(s.toUpperCase(), s.toLowerCase())
      val names =List("Peter","Paul","Marry")
      println(   names.map(ulcase)  )    //List(Vector(PETER, peter), Vector(PAUL, paul), Vector(MARRY, marry))
      //flatMap先map，再扁平化
      println(   names.flatMap(ulcase)  )    //List(PETER, peter, PAUL, paul, MARRY, marry)
      
      
      
      
      //排序
      println(list1.sorted) //List(Cool, rule, tools)
      println(list1.reverse)
      
      
      //collect
      //collect方法用于偏函数，那些并没有对所有可能的输入值进行定义的函数
      //它的产出被定义的所有参数的函数值的集合
      println(   "-3+4".collect{case '+' => 1;case '-' => -1 }   )  //Vector(-1, 1)
      
      
      //zip  得到两个列表的对偶列表
      val prices = List(5.0, 20.0, 9.95)
      val quantities = List(10, 2, 1)
      println( prices zip quantities )    //List((5.0,10), (20.0,2), (9.95,1))
      println( (prices zip quantities).map { p => p._1*p._2 } )    //List(50.0, 40.0, 9.95)  得到价格列表
      println( ((prices zip quantities).map { p => p._1*p._2 }) sum )    //99.95  得到总价
      println( ((prices zip quantities).map { p => p._1*p._2 }).sum )    //99.95  得到总价
      
      
      
      //zipAll方法让你指定较短列表的缺省值
      println (  List(5.0, 20.0, 9.95).zipAll(List(10, 2), 0.0, 1)  )   //List((5.0,10), (20.0,2), (9.95,1))
      println (  List(5.0, 20.0).zipAll(List(10, 2, 5), 0.0, 1)  )   //List((5.0,10), (20.0,2), (0.0,5))
      
      //zipWithIndex返回元素与对应位置的列表
      println(  "Scala".zipWithIndex   )     //Vector((S,0), (c,1), (a,2), (l,3), (a,4))
      println(  "Scala".zipWithIndex.max   )     //(l,3)   //比较的是第一个值的max
      println(  "Scala".zipWithIndex.max._2   )     //3   //比较的是第一个值的max的下标
      
      
      val listSplit = Array(1,2,3,4,5,6,7,8,9)
      println( listSplit.slice(4, 8).mkString("\t")) //5	6	7	8
      println( listSplit.slice(4, 8)) //[I@7d4991ad
      println( listSplit.toSeq.slice(4, 8) )  //WrappedArray(5, 6, 7, 8)
      
      
      //数组的合并
      val l1 = Array(1,2,3,4,5)
      val l2 = Array(6,7,8)
      val l12 = Array.concat(l1,l2)
      println(l12.mkString(","))  //1,2,3,4,5,6,7,8
      
      //二维数组
      var myMatrix = Array.ofDim[Int](3, 3)
      for(i <- 0 to 2)
          for( j <- 0 to 2){
        	  myMatrix(i)(j) = j
          }
      for(i <- 0 to 2) {
         for( j <- 0 to 2){
        	print(" "+myMatrix(i)(j))
         }
         println()
      }
          
      //tabulate 从0开始，依靠后面的函数生成数组，生成n个
      val x =  Array.tabulate(5)(_+1)
      println(x.mkString(" ")) //1 2 3 4 5
      
      val x2 =  Array.tabulate(5)(_*2)
      println(x2.mkString(" ")) //0 2 4 6 8
      
      val random = new Random()
      val x3 =  Array.tabulate(5)(_=>random.nextGaussian)
      
      //排列组合
      val list = List(1,2,3)
      val combs = list.combinations(2)  //n维排列组合。An Iterator which traverses the possible n-element combinations of this list.
      combs.foreach (println)
//List(1, 2)
//List(1, 3)
//List(2, 3)
      
      
      val trueList = Array.fill(3)(true)
      trueList.foreach { print }
      println
      
      
      val twoDimensional  = Array.fill(2, 3)(0)
      twoDimensional.foreach { x => println(x.mkString(",")) }
      
      
      
      
      //list数据分组
      val listdata = Array(1,2,3,10,5,4,6,7,9,8)
      //排序
      val dataSorted = listdata.sorted
      println(dataSorted.toBuffer)
      //翻转
      dataSorted.reverse
      //分组
      val dataGroup = dataSorted.grouped(4) //让数据4个 为一组 Iterator[Array[Int]]
      val dataGroupList = dataGroup.toList //二维数组 List[Array[Int]]
      dataGroupList.map(_.size).foreach { print} //442
      dataGroup.foreach { println }  //因为dataGroupp外层是Iterator类型的，经过toList方法后，迭代器指针已经指向了数据的末位，所以没有任何信息的打印
      println
      println("*"*8)
      val dataGroup2 = dataSorted.grouped(4)
      dataGroup2.foreach { x => println(x.toList) }
//List(1, 2, 3, 4)
//List(5, 6, 7, 8)
//List(9, 10)
      
      
      //将二维数组压平，二维数组转换为一维数组
      val  dataList = dataGroupList.flatten //List[Int]
      println(  dataList.mkString(",") )  //1,2,3,4,5,6,7,8,9,10
      
      
      //先空格切分，再压平
      val lines = List("hello tom hello jerry","hello word","hello jerry")
      val data = lines.flatMap(_.split(" "))
      //统计单词出现次数                 (单词，次数)元组            按单词分组   
      val word_ListMap = data.map((_,1)).groupBy(_._1) //Map[String, List[(String, Int)]]
      //                    遍历次数的组，类型list 次数组数字相加   第一个 _是上次计算结果   第二个_是遍历的每一个元组                                                       
      val result = word_ListMap.mapValues(_.foldLeft(0)(_+_._2))
      
      
      //foldLeft
      val t = List(1,2,3,5,5)
      println( t./:(0)({
        (sum,num)=>println(sum +"--"+num) 
        sum+num
      })
      )
//0--1
//1--2
//3--3
//6--5
//11--5
//16
      
      result.foreach(println)
      //按照出现次数排序                                      递减
      val resultSorted = result.toList.sortBy(_._2).reverse
      resultSorted.foreach(println)
      
      
      //求和 //并行数组 .par
      val data_1 = Array(1,2,3,4)
      println( data_1.sum ) //10 单线程求和
      println( data_1.par.reduce(_+_) )  //10 多线程求和
      
      println( data_1.par.fold(0)(_+_) ) //10
      
      println(data_1.par.fold(10)(_+_) ) //50  //多了40，是因为本机是4核的，开启了与核数相同的线程数并行计算，每个线程加10，所以多了40
      
      
      
      
      //求数组最大值
      val max = data_1.reduce(Math.max(_, _))
      println("max="+max)
      
      
  
      
      //聚合
      val arrlist = List( List(1,2,3), List(3,4,5), List(2), List(0))
      //先局部求和，然后在统计求和    第一个_表示初始值或以后计算的中间结果  第二个_表示其中的每个元素 ， 第三个_表示上步计算的每个局部的结果
      //aggregate的第二个括号有两个参数：第一个参数是对局部进行操作，第二个参数是对上步计算局部的结果进行操作
      val arrlist_res1 = arrlist.aggregate(0)(_+_.sum, _+_) //聚合
      println(arrlist_res1)  //20
      
      
      val arrlist_res2 = arrlist.aggregate(10)(_+_.sum, _+_) //聚合
      println(arrlist_res2)  //30
      
      val arrlist_res3 = arrlist.par.aggregate(10)(_+_.sum, _+_) //聚合
      println(arrlist_res3)  //60  //因为有三个 非空 list，每个list都会初始化是加10
      
      
      
      
      //并集，允许重复
      val ll1 = List(5,6,4,7)
      val ll2 = List(1,2,3,4)
      println( ll1.union(ll2) ) //List(5, 6, 4, 7, 1, 2, 3, 4)
      println( ll1 union ll2 ) //List(5, 6, 4, 7, 1, 2, 3, 4)
      
      
      //求交集
      println( ll1.intersect(ll2) )  //List(4)
      
      //差集
      println( ll1.diff(ll2) )   //List(5, 6, 7)
      
      
      
      
      
    }//main
  
  
}