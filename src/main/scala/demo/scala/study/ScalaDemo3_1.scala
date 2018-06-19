package demo.scala.study

/**
 * @author qingjian
 */
object ScalaDemo3_1 {
    def main(args:Array[String]):Unit={
        /**
        * 映射和元组
       */
        //不可变的Map[String,Int]
        val scores = Map("Alice"->10,"Bob"->3,"Cindy"->8)
        //可变的映射
        val scores2 = new scala.collection.mutable.HashMap[String,Int]()
        /*
         * ->操作符用来创建对偶
         * "Alice"->10 产出是  ("Alice",10)
         */
        val scores3=Map(("Alice",10),("Bob",3),("Cindy",8))        
        
        /*
         * 获取映射值
         */
        val bobscore = scores("Bob")
        println(bobscore)    //3
        //val bobscore2 = scores("Bob2")
        //println(bobscore2)    //报错了 Exception in thread "main" java.util.NoSuchElementException: key not found: Bob2
        
        //所以需要先判断contain操作 
        val bobscore3=if(scores.contains("Bob2")) scores("Bob2") else 0
        println(bobscore3) //0
        
        //上两步的简便写法
        val bobscore4 = scores.getOrElse("Bob2", 0) //0
        
        /**
         * 更新映射值
         * 在可变映射中，可以更新某个映射的值
         * 或者添加一个新的映射关系
         */
        //报错 scores("Bob")=30
        
        println(scores2)    //Map()
        scores2("Bob")=30
        println(scores2)    //Map(Bob -> 30)  //添加
        
        scores2("Bob")=3
        println(scores2)    //Map(Bob -> 3)    //更新
        
        //也可以使用+=添加或更新多个关系
        scores2+=("Bob"->5,"Fred"->7)
        println(scores2)    //Map(Bob -> 5, Fred -> 7)
        
        val newScore = scores2+("Bob"->6,"Mary"->8)
        println(newScore)    //Map(Bob -> 6, Fred -> 7, Mary -> 8)
        
        //-=移除映射
        newScore-="Mary"
        println(newScore)    //Map(Bob -> 6, Fred -> 7)
        
        
        
        /**
         * 迭代映射
         * for((k,v)<- 映射) 处理k和v
         */
        
        
        /**
         * 已排序映射
         */
        
        val scores5=scala.collection.immutable.SortedMap("Alice"->10,"Fred"->7,"Bob"->3,"Cindy"->8)
        println(scores5)    //Map(Alice -> 10, Bob -> 3, Cindy -> 8, Fred -> 7)
        
        /**
         * 元组
         * 元组的值是通过单个的值包含在圆括号中构成的
         * (1,3.14,"Fred")
         */
        val t=(1,3.14,"Fred")
        
        //访问 _1,_2,_3
        val second=t._2
        println(second)//3.14
        
        //也可以使用模式匹配来获取元组的组元
        val (first,seconds,third)=t
        println(first)    //1
        //如果并不所有的部件都需要，那么可以在不需要的部件位置上使用_
        val (first2,second2,_)=t
        //元组可以用于函数需要返回不止一个值的情况
        //StringOps的partition方法返回的是一对字符串
        var re = "New York".partition { x => x.isUpper }
        println(re)    //(NY,ew ork)
        var re2 = "New York".partition {_.isUpper}
        println(re2)
        
        
        
        
        /**
         * 拉链操作 zip
         */
        val symbols=Array("<","-",">")
        val counts=Array(2,10,2)
        val pairs=symbols.zip(counts) //Array(("<",2),("-",10),(">",2))
        
        for((s,n)<-pairs) Console.print(s,n)  //(<,2)(-,10)(>,2)
        
        //toMap方法可以将对偶的集合转换成映射 keys.zip(values).toMap
        
        
        
        
    }
   
    
    
}