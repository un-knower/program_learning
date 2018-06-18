package demo.scala.study

import scala.collection.mutable.ArrayBuffer

/**
 * @author qingjian
 */
object ScalaDemo2_1 {
  def main(args: Array[String]): Unit = {
      /**
     * 定长数组
     */
    val nums=new Array[Int](10) //10个整数的数组，所有元素初始化为0
    val str = Array("hello","world","we","are","one")
    //使用()访问元素，而不是[]
    println(str(0))  //hello
    str(0)="good" //改变了该值
    
    //遍历
    for(i <- str) {
      print(i+" ") //good world we are one 
    }
    println()
    /**
     * 变长数组：数组缓冲
     * 
     */
    val b = ArrayBuffer[Int]()
    //或者
    val b2 = new ArrayBuffer[Int]()
    //或者
    val b3 = new ArrayBuffer[Int]
    
    //+=在尾端添加元素
    b+=1
    //+=添加多个元素，以括号括起来
    b+=(1,2,3,4,5)
    //++=合并任何集合
    b++=Array(6,7,8)
    
    for(i<- 0 to b.length-1) {
      print(b(i)+" ") //1 1 2 3 4 5 6 7 8 
    }
    println()
    //移除最后5个
    //在数组缓冲的尾部添加或者移除元素是一个高效的操作
    b.trimEnd(5)  //4
    println(b.length)
    for(i<- 0 to b.length-1) {
      print(b(i)+" ") //1 1 2 3  
    }
    println()
    //在任意位置插入或者移除元素
    b.insert(2,6)  //在下标2添加6
    for(i<- 0 to b.length-1) {
      
      print(b(i)+" ") //1 1 6 2 3
    }
    println()
    b.insert(2,7,8,9) //在下标2之前添加7,8,9。从0开始
    for(i<- 0 to b.length-1) {
      print(b(i)+" ") // 1 1 7 8 9 6 2 3
    }
    println()
    
    b.insert(b.length,4) //在末尾添加4。从0开始
    for(i<- 0 to b.length-1) {
      print(b(i)+" ") // 1 1 7 8 9 6 2 3 4
    }
    println()
    
    //b.insert(b.length+1,4) //在末尾+1处添加4。从0开始 //报错
    
    b.remove(2)  //删除下标为2的元素，从0开始
    for(i<- 0 to b.length-1) {
      print(b(i)+" ") // 1 1 8 9 6 2 3 4
    }
    println()
    
    b.remove(2,3) //从下标2开始，删除3个元素，从0开始
    for(i<- 0 to b.length-1) {
      print(b(i)+" ") //1 1 2 3 4
    }
    println()
    
    //将ArrayBuffer转换为Array
    b.toArray
    
   /**
    * 遍历数组
    */
    for(i<-0 until b.length) {
      print(b(i)+" ")
    }
    println()
    for(i<-0 to b.length-1) {
        print(b(i)+" ")
    }
    println()
    for(ch<-b) {
      print(ch+" ")
    }
    println()
    
    //每两个元素一跳
    for(i<-0 to (b.length,2))
    {
    	print(b(i)+" ")  // 1 2 4 
    }
    println()
    
    //从尾部遍历
    for (i<-(0 until b.length).reverse) {
      
    }    
    
    /**
     * 数组转换
     */
    val a = Array(2,3,5,7,11)
    var result = for(elem<-a) yield 2*elem
    //result是Array(4,6,10,14,22)
    
    //遍历一个集合时，可以处理那些满足条件的元素
    result = for(elem<-a if elem%2==0) yield 2*elem
    //Array[Int] = Array(4)
    //原数组并没有收到影响
    //也可以这么写
    result = a.filter(_%2==0).map(_*2)  //Array[Int] = Array(4)
    
    println("********例子*********")
    /*
    * 例：移除除第一个负数之外的所有负数
    */
    //方法1
    var testArr = ArrayBuffer(1,2,-3,4,-5,6,-7,-8)
    /*
     * 报错
     * 
    var first=true;
    for(i <- 0 until testArr.length) {
      if(testArr(i)<0) {
        if(first) {
          first=false
        }
        else {
          testArr.remove(i)
        }
      }
    }*/
    
    //不能用for(i <- 0 until testArr.length)，这样删除元素之后就会报边界溢出
    //修改如下，同样报错
   /* var first=true;
    var n = testArr.length
    for(i <- 0 until n) {
      if(testArr(i)<0) {
        if(first) {
          first=false
        }
        else {
          testArr.remove(i)
          n-=1
        }
      }
    }
    */
    //修改为while
    var first = true
    var n = testArr.length
    var i =0
    while(i<n) {
      if(testArr(i)<0) {
       if(first) {
         first=false
         i+=1
       }
       else { //不是第一个负数
         testArr.remove(i)
         n-=1
       }
       
      }else {
         i+=1
       }
    }
    
    //打印testArr
    for(elem<-testArr) {
      print(elem+" ")
    }
    println() //1 2 -3 4 6 
    
    
    
    //上述方法并不好，从数组缓冲中移除元素并不高效，把非负数值拷贝到前端要好得多
    //方法2
    testArr = ArrayBuffer(1,2,-3,4,-5,6,-7,-8)
    first = true
    val indexes = for(i<-0 until testArr.length if first||testArr(i)>=0) yield {
      if(testArr(i)<0) first=false
      i
    }
    for(elem<-indexes) {
      print(elem+" ")    //0 1 2 3 5  //将满足条件的数的下标找出来
    }
    println()
    //然后将元素移动到该去的位置，并截断尾端
    for(j<-0 until indexes.length) testArr(j)=testArr(indexes(j))
    testArr.trimEnd(testArr.length-indexes.length)
    //打印输出
    for(elem<-testArr) {
      print(elem+" ")    //1 2 -3 4 6 
    }
    println()
    
    
    
    /**
     *  数组的常用算法 
     */
     println(Array(1,2,3,4).sum)    //10
     println(ArrayBuffer("Mary","had","a","little","lamb").max) //little
     val arr = Array(1,7,2,9)
    // val arrSorted = arr.sorted(_>_)
     scala.util.Sorting.quickSort(arr) //返回Unit，改变原数组arr的值
     for(elem<-arr) {
       print(elem+" ")    //1 2 7 9 
     }
     println()
     
     //显示数组的内容
     println(arr.mkString(" and "))    //1 and 2 and 7 and 9
     println(arr.mkString("<",",",">"))    //<1,2,7,9>
     
     //和toString相比
     println(arr.toString())        //[I@154617c
     println(arr.toArray)        //[I@154617c
      
     /**
      * 多维数组
      * 多维数组是通过数组的数组来实现的
      * Array[Array[Double]] 
      */
      val matrix = Array.ofDim[Double](3,4) //3行，4列
      //访问其中的元素
      //matrix(row)(column)=42
      println(matrix(2)(3))  //0.0
      
      //也可以创建不规则的数组，每一行的长度各不相同
      val triangle = new Array[Array[Int]](10)
      for(i<-0 until triangle.length) {
          triangle(i)=new Array[Int](i+1)
      }
      
      /**
       * 与java数组的交互
       */
      
      
      
      
  }
}