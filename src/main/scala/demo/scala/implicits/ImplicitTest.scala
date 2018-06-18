package demo.scala.implicits

/**
 * @author qingjian
 */

import FrenchPunctuation._  ////引入FrenchPunctuation的implicit val 
object ImplicitTest {
    

    def quote(what: String)(implicit delims: Delimiter) = delims.left+what+delims.right
  
    def main(args: Array[String]): Unit = {
        
        val wh = "hello"
        println(quote(wh)) //    <<hello>>
        println(quote(wh)(Delimiter("|","|"))) //    |hello|
        
        
      val a:Integer = 1
      
    }
}