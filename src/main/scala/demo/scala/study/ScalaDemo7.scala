package demo.scala.study

import java.io.File
import java.net.URL

/**
 * @author guangliang
 */
object ScalaDemo7 {
  /**
   * 进程控制
   */
    /**
     * scala编写shell脚本
     */
    import sys.process._
    def main(args: Array[String]): Unit = {
    		//这样做是 ls -al . 命令被执行
    		"ls -al ." !  
            /**
             * sys.process包包含了一个从字符串到ProcessBuilder对象的隐式转化
             * !操作符执行的就是这个ProcessBuilder对象
             * !操作符返回的结果是被执行程序的返回值：程序成功执行的话就是0，否则就是显示错误的非0值
             * 
             */
    		
            /**
             * 如果使用的是!!而不是!，输出会以字符串的形式返回。
             */
            val result = "ls -al ." !!
            
            /**
             * 还可以经程序的输出以管道形式作为输入传送到另一个程序，用#|操作符
             */
            
            "ls -al ." #| "grep seo" !
            
            /**
             *  #> 重定向
             */
            
            "ls -a ." #> new File("output.txt")
            
            
            /**
             * 追加到文件末尾，而不是从头覆盖的话，用 #>>
             */
            "ls -a ." #>> new File("output.txt")
            
            /**
             * 把某个文件的内容作为输入，使用#<
             */
            "grep sec" #< new File("output.txt")
            "grep sec" #< new URL("http://www.baidu.com")
            
            
            
            
    }
    
    
    
    
}