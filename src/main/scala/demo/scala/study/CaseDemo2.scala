package demo.scala.study

/**
 * @author qingjian
 * 
 * 匹配嵌套结构
 */



object CaseDemo2 {
   abstract class Item
   case class Article(description:String, price:Double) extends Item  //文章
   case class Bundle(description:String, discount:Double, items:Item*) extends Item //捆绑商品
   def main(args: Array[String]): Unit = {
       val items = Bundle("Father's day special", 20.0,
           Bundle("Anchor Distillery", 10.0,
               Article("Hadoop",19.5),        
               Article("Flink",30.1),        
               Article("Java",39.5)        
           )
       )
       
       /**匹配嵌套模式*/
       items match {
           case Bundle(_,_, Article(desc ,_),_*) => println(desc)
           case _ => println("other")
       } //other
       
       items match {
           case Bundle(_,_, Bundle(desc ,_,_*),_*) => println(desc)
           case _ => println("other")
       } //Anchor Distillery
     
       /**使用@绑定变量*/
       
       items match {
           case Bundle(_,_,bun @ Bundle(_ ,_,_*),res @ _*) => println(bun.description)
           case _ => println("other")
       } //Anchor Distillery
       
       
       
       
   }
  
}