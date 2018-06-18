package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo8 {
  /**
   * 正则表达式
   */
    def main(args: Array[String]): Unit = {
          /**
         * 构造一个Regex对象，用String类的r方法即可
         */
        val numPattern = "[0-9]+".r
        /**
         * 如果正则表达式包含反斜杠或引号的话，
         * 那么最好使用“原始”字符串语法 """..."""  
         */
        val wsnumwsPattern = """\s+[0-9]+\s+""".r
        
        /**
         * findAllIn 方法返回遍历所有匹配项的迭代器
         */
        for(matchString <- numPattern.findAllIn("99 bottles,98 bottles")) {
            println(matchString)
            //99
            //98
        }
        
        
        val matches = numPattern.findAllIn("99 bottles,98 bottles").toArray
        println(matches.mkString) //9998
        
        /*
         * 首个匹配项 findFirstIn
         */
        val ml = numPattern.findFirstIn("99 bottles,98 bottles")
        println(ml) //Some(99)
        
        
        /**
         * 替换
         */
        val r1 = numPattern.replaceFirstIn("99 bottles,98 bottles", "XX")
        println(r1)    //XX bottles,98 bottles
        val r2 = numPattern.replaceAllIn("99 bottles,98 bottles", "XX")
        println(r2)    //XX bottles,XX bottles
    }
    
    
    
}