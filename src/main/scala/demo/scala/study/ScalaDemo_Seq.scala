package demo.scala.study

/**
 * @author qingjian
    
 */

//Seq的缺省实现是List
//IndexSeq的缺省实现是Vector
//
object ScalaDemo_Seq {
  def main(args: Array[String]): Unit = {
      var seq1 = Seq(1,2,3,4,4)
      println(seq1)   //List(1, 2, 3, 4, 4)
      
      println( seq1.contains(3) )    //true
      println(  seq1.startsWith(Seq(1))  )  //true
      println(  seq1.startsWith(Seq(1,2))  )  //true
      println(  seq1.startsWith(Seq(2))  )  //false
      println(  seq1.endsWith(Seq(2))  )  //false
      
      println( seq1.indexOf(4))    //3
      println( seq1.indexOf(7))    //-1
      println( seq1.lastIndexOf(4))    //4
      
      println( seq1.indexOfSlice(Seq(2,3)))   //1
      println( seq1.lastIndexOfSlice(Seq(4,5)))    //-1
      
      
      println( seq1.reverse)  //List(4, 4, 3, 2, 1)
      println( seq1.diff(Seq(1,4)))    //List(2, 3, 4)
      
      println(seq1.slice(2, seq1.size)) //List(3, 4, 4)
      
      
      
      
      
      
      
      
      
      
      
      
  }
}