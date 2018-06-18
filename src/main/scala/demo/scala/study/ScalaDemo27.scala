package demo.scala.study

object ScalaDemo27 {
    def main(args: Array[String]): Unit = {
        val str = """This is
          a scala multiline
          
          String"""
        println(str)
//This is
//          a scala multiline
//          
//          String
        //发现多行数据使用"""，位置不会对其
        //如果需要每一行固定对齐，解决该问题的方法是使用stripMargin，stripMargin默认是|切分
        
        val str2 = """|This is
          |a scala multiline
          |String
          """.stripMargin
        println(str2)
//This is
//a scala multiline
//String
        
        //当然stripMargin方法也可以自己指定“定界符”,同时更有趣的是利用stripMargin.replaceAll方法，还可以将多行字符串”合并”一行显示。
        val speech = """Let us scala and

            |learn spark oh""".stripMargin.replaceAll("\n", " ")
        println(speech)
//Let us scala and  learn spark oh
        
    }
  
}