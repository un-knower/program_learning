package demo.scala.study

/**
 * @author guangliang
 */


class Document {
    def setTitle(title:String) = {"set:"+title;this}
    def setAuthor(author:String) = {"set:"+author;this}
}

class Document2 {
    def setTitle(title:String):this.type = {"set:"+title;this}
    def setAuthor(author:String):this.type = {"set:"+author;this}
}

class Book extends Document{
    def addChapter(chapter:String)={"set:"+chapter;this}
}

class Book2 extends Document2{
	def addChapter(chapter:String)={"set:"+chapter;this}
}


object ScalaDemo18 {
    def main(args: Array[String]): Unit = {
      var doc = new Document
      doc.setTitle("t").setAuthor("a")  //返回this 的方法，可以把方法串起来：方法串接
      var book = new Book  //声明Document的子类
      book.addChapter("c").setAuthor("a")
      //book.setAuthor("a").addChapter("c") //报错
      
      var book2 = new Book2  //声明Document的子类
      book2.addChapter("c").setAuthor("a")
      book2.setAuthor("a").addChapter("c") //不报错，book2.setAuthor返回类型是book.type
      
      
      
      
      
      
      
    }
}