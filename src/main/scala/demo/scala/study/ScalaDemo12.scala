package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo12 {
    def main(args: Array[String]): Unit = {
      /**
     * 集合类
     *                Iterable
     *                   |    
     *   Seq----------  Set------ Map
     *    |              |         |
     *   IndexedSeq  SortedSet   SortedMap
     *   
     *    
     *    不可变序列
     *                  Seq
     *      |                  |       |       |      |
     *  IndexedSeq            List   Stream  Stack  Queue               
     *   |         |
     * Vector    Range
     *   
     *   
     *   
     *   可变序列
     *   
     *                  Seq
     *       |               |        |      |       |
     *   IndexedSeq         List    Stream  Stack  Queue
     *    |      |
     * Vector   Range               
     *   
     */
    
        
        //列表
        //要么是空表Nil
        val digits = List(4,2)    
        print(digits.head) //4
        print(digits.tail) //List(2)
        
        // ：：操作符从给定的头和尾创建一个新的列表
        9::List(4,2)  //List(9,4,2)
        
        9::4::2::Nil  //List(9, 4, 2)
        
        //是右结合，通过::操作符，列表将从末端开始构建
        9::(4::(2::Nil))
        //遍历列表，可以用迭代器或使用递归
        def sum(lst:List[Int]):Int={
            if (lst==Nil) 0
            else lst.head+sum(lst.tail)
        }
       println()
       println( List(9,4,2).sum )    //15
        
       val lst = scala.collection.mutable.LinkedList(1,-2,7,-9)
       var cur = lst
       
       while(cur!=Nil) {
           print(cur.elem+"\t") //1 -2  7   -9
           cur=cur.next
       }
       println()
       cur=lst
       while(cur!=Nil && cur.next!=Nil) {
        	print(cur.elem+"\t") //1  7 
        	cur=cur.next.next
       }
       
       
       /**
        * 集
        * 集合是不重复元素的集合，且是无序，集是以哈希集实现的，不保留元素插入顺序
        * 其元素根据hashCode方法的值进行组织
        */
       
       Set(2,0,1)+1   //Set(2,0,1)
       println( scala.collection.immutable.SortedSet(4,5,1,3,2) )  //TreeSet(1, 2, 3, 4, 5)
       
       val digitsSet = Set(1,7,2,9)
       //是否包含
       println ( digitsSet contains(0) ) //false
       println( digitsSet.contains(7) )  //true
       //是否子集
       println( Set(1,2).subsetOf(digitsSet) )    //true
       
       /*
        * 集合的 并差交
        * 并：union ++
        * 差：diff  --  &~
        * 交：intersect &
        */
       
       val primes = Set(2,3,5,7)
       println(digitsSet) //Set(1, 7, 2, 9)
       //并
       println ( digitsSet union primes )   //Set(5, 1, 9, 2, 7, 3)
       println ( digitsSet ++ primes )   //Set(5, 1, 9, 2, 7, 3)
       println ( digitsSet | primes )   //Set(5, 1, 9, 2, 7, 3)
       
       //交
       println ( digitsSet.intersect(primes) )    //Set(7, 2)
       println ( primes.intersect(digitsSet) )    //Set(2, 7)
       println ( primes & digitsSet )    //Set(2, 7)
       
       
       //差
       println ( digitsSet -- primes )    //Set(1, 9)
       println ( primes -- digitsSet )    //Set(3, 5)
       println ( primes.diff(digitsSet) )    //Set(3, 5)
       println ( primes &~ digitsSet )    //Set(3, 5)
       
       
       
       
        
    }
    
    
    
    
    
}