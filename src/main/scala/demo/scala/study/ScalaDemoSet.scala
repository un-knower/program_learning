package demo.scala.study

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @author qingjian
 */
object ScalaDemoSet {
    /**
     *集合 set
     * 一般而言，
     * +用于将元素添加到无先后顺序的集合
     * -用于删除元素
     */
    def main(args: Array[String]): Unit = {
   		
   		println( Set(1,2,3)+5 )    //Set(1, 2, 3, 5)
   	    
        var set1 = Set(1,2,3)
        set1+=5
        println(set1)    //Set(1, 2, 3, 5)
        set1+=5
        println(set1)    //Set(1, 2, 3, 5)
        
        
        val numbers  = ArrayBuffer(1,2,3,4)
        numbers+=3
        println( numbers )  //ArrayBuffer(1, 2, 3, 4, 3)
        
        
        var numbers2 = Set(1,2,3)
        numbers2+=5
        var numberVector = Vector(1,2,3)
        numberVector:+=5
        
        //移除元素
        println( Set(1,2,3)-2 )        //Set(1, 3)

        /**
         * 添加和移除元素的操作符
         * 1.向后:+或向前+:追加元素到序列当中
         * 2.添加+元素到无先后次序的集合中
         * 3.用-移除元素
         * 4.用++和--来批量添加和移除元素
         * 5.对于列表，优先使用::和:::
         * 6.改值操作有+=、++=、-=和--=
         * 7.对于集合，我更喜欢++、&和--
         * 8.脊梁不用++:、+=和++=
         */
        
        /**
         * 化简、折叠和扫描
         */
        println( List(1,7,2,9).reduceLeft(_-_) )  //-17   ((1-7)-2)-9
        println( List(1,7,2,9).reduceRight(_-_) )  //-13    1-(7-(2-9))   
    
        println( List(1,7,2,9).foldLeft(0)(_-_) )  //-19   (((((0-init)-1)-7)-2)-9)
        println( List(1,7,2,9).foldRight(0)(_-_) )  //-13   (1-(7-(2-(9-(0-init)))))
        
        
        /**
         * 拉链操作
         */
        val prices = List(5.0,50.0,9.95)
        val quantities = List(10,2,1)
        prices zip quantities    //List[(Double, Int)] = List((5.0,10), (50.0,2), (9.95,1))
        //每个物品价钱
        println ( (prices zip quantities).map { p => p._1*p._2 })   // List(50.0, 100.0, 9.95)
        //物品总价
        println ( (prices zip quantities).map { p => p._1*p._2 }.sum )    //159.95
        
        //zipAll 指定段列表的缺省值，第二个参数补充左边，第三个参数补充右边
        println( List(5.0,20.0,9.95).zipAll(List(10,2),0.0,1) )    //List((5.0,10), (20.0,2), (9.95,1))
        
        println( List(1,1).zipAll(List(2),6,7) )    //List((1,2), (1,7))
        println( List(1).zipAll(List(2,3),6,7) )    //List((1,2), (6,3))

        
        //zipWithIndex 返回对偶的列表，其中每个对偶第二个组成部分是每个元素的下标
        println( "Scala".zipWithIndex )    //Vector((S,0), (c,1), (a,2), (l,3), (a,4))
        println( "Scala".zipWithIndex.max )   //(l,3)
        println( "Scala".zipWithIndex.max._2 )   //3
        
        
        /**
         * 迭代器
         * 
         */
        /*
        while(iter.hasNext) {
            iter.next()
        }
        for(elem<-iter) {
            //对 elem进行操作
        }
        */
        
        /**
         * 流
         * 流是一个尾部被懒计算的不可变列表--只有当你需要时它才会被计算
         */
        def numsFrom(n:BigInt):Stream[BigInt]= n #::numsFrom(n+1)
        val tenOrMore = numsFrom(10)
        println( tenOrMore.tail )    //Stream(11, ?)
        println( tenOrMore.tail )    //Stream(11, ?)
        println( tenOrMore.tail )    //Stream(11, ?)
        println( tenOrMore.tail.tail )    //Stream(12, ?)
        
        val squares = numsFrom(1).map(x=>x*x)
        //如果想得到多个答案 调用take，然后用force
        println( squares.take(5).force )    //Stream(1, 4, 9, 16, 25)
        
        //别执行   squares.force  这个调用将会对一个无穷流的所有成员进行求值，引发OutofMemoryError
        
        //可以从迭代器构造一个流。Source.getLines方法返回一个Iterator[String]
        //用这个迭代器，对于每一行你只能访问一次，而流将缓存访问过的行，允许你重新访问他们
        /*
         <?php
            $a=1;
            print $a; 
         ?>
         */
        
        val words = Source.fromFile("src/scalaStream.php").getLines().toStream
        println (  words )   //Stream(<?php, ?)  //第一行
        println (  words )   //Stream(<?php, ?)   //第一行
        println (  words(0) )   //<?php   //第1行
        println (  words(1) )   //$a=1;  //第2行
             
        /**
         * 懒视图
         */
        //view方法产出的方法总是被懒执行的集合
        val powers = (0 until 1000).view.map(Math.pow(10,_))
        println ( powers(2) )    //100.0   这里pow(10,2)被计算，但其他值的幂并没有被计算
        //和流一样，用force方法可以对懒视图强制求值。
        //懒集合对于处理那种需要以多种方式进行变换的大型结合是很有好处的，因为它避免了构建出大型中间集合的需要
        (0 to 1000).map(Math.pow(10,_)).map(1/_)
        
        
        /**
         * 并发集合
         * 
         * 集合.par
         * 
         */
        
        for(i<- (0 until 10).par){
            print(i+" ") //2 5 1 0 6 3 4 7 8 9  .par并行化for循环
        }
        
        //如果并行运算改变了共享的变量，则结果无法预知
        //不要更新一个共享的计数器
   		println();
        var count=0
        for(c<-(0 until 10).par) {
            if (c%2==0) {
                count+=1
                //println( count )
            }
        }
        println( count ) //5
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
    }
    
    
    
    
    
}