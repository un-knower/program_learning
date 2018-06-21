package demo.spark.streaming.mllib

object ProfileALS_test {
    def main(args: Array[String]): Unit = {
        //span test
        val str = "a\tb\tc d"
        val (l,r) = str.span(_!='\t') //从第一个字符开始，直到不为true（遇到第一个\t）作为一部分，剩下作为第二部分，当然包括前面的分隔符\t，所以第二部分要trim
        println(l)
        println(r)
        println(r.trim) //去掉分隔符
    }
  
}