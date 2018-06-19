package demo.scala.study

import scala.io.Source
import java.io.PrintWriter
import java.io.File

/**
 * @author guangliang
 */

/**
 * 文件读写
 */

object ScalaDemo6 {
    def main(args: Array[String]): Unit = {
    	var source = Source.fromFile("file/myfile","UTF-8") 
        /**
         * 读取多行
         */
        val contentIterator=source.getLines()
        for(l<-contentIterator){
            println(l)
        }
        /*
         * 读取单个字符
         */
    	source = Source.fromFile("file/myfile","UTF-8")        
        for(c<-source) {
            println(c) //显示单个字符
        }
        /**
         * 将整个文件读取成一个字符串
         */
    	source = Source.fromFile("file/myfile","UTF-8") 
        val contentArr = source.getLines().toArray
        println(contentArr.mkString) //wangwu,5zhaoliu,6zhangsan,3
        
        /**
         * 如果想查看某个字符但又不处理它的话
         * 调用source对象的buffered方法
         * 这样就可以用head方法查看下一个字符，但同时并不把它当做是已处理的字符
         */
       /*
        * source = Source.fromFile("file/myfile","UTF-8")
        val contentBuff = source.buffered
        while(contentBuff.hasNext) {
            var c = contentBuff.next()
            if(contentBuff.head=='a') { //这里会报错哦
                println(c)
            }
        }
        */
        /**
         * 读取二进制文件
         * val file = new File(filename)
         * val in = new FileInputStream(file)
         * val bytes = new Array[Byte](file.length)
         * in.read(bytes)
         * in.close()
         */
        
        
        /**
         * 写入文本文件
         */
        /*
        val out = new PrintWriter("file.txt")
        for(i<- 1 to 100)
            out.println(i)
        out.close()
        
                        或者
       out.printf("%6d %10.2f",format(quantity,price))
        
        */
        
        /**
         * 访问目录
         * Scala没有“正式”给出访问某个目录中的所有文件，或者递归遍历所有目录的类。
         */
        def walk(file:File) {
         if(file.isFile()) {
             println(file.getName)
             
         }  
         else {
             file.listFiles().foreach { walk }
         }
        }
        
        walk(new File("file/myfile"))
        
        
        /**
         * 序列化
         * 如果能接受缺省的ID，可以略去@SerialVersionUID的注解
         */
         @SerialVersionUID(42L) class PersonSer extends Serializable    
        
        
    }
    
    
 
  
}