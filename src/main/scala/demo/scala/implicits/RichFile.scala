package demo.scala.implicits

import java.io.File
import scala.io.Source

/**
 * @author qingjian
 */
class RichFile(val from:File) {
    def read = Source.fromFile(from.getPath).mkString
  
}

object RichFile {
    //隐式转换方法
    implicit def file2RichFile(from:File) = new RichFile(from)
}


object MainApp {
    def main(args: Array[String]): Unit = {
        //导入隐式转换
        import RichFile._
        
        val file = new File("src/scalaStream.php")
        val content = file.read  //会到隐式上下文去找read方法，发现File能变成RichFile，RichFile有read方法
        println(content)
        
        
    }
    
    
}
