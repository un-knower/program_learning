package demo.scala.implicits

/**
 * @author qingjian
 */
case class Delimiter(left:String , right:String)
object FrenchPunctuation {
    
    implicit val quoteDelimiters = Delimiter("<<",">>")
        
    
}