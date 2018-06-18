package demo.scala.study

/**
 * 元组Tuple
 * 元组不可变，可以包含不同类型的元素
 * 
 */
object Scala_Tuple {
    def main(args:Array[String]):Unit= {
        val pair = (99, "Luftballons") //Tuple2[Int, String]
        println(pair._1+" : "+pair._2) //99 : Luftballons
        
        println(pair.productIterator.mkString(",")) //99,Luftballons
        println("prefix="+pair.productPrefix) //Tuple2
        println("prefix="+(1,"2",3.0).productPrefix) //Tuple3
        
        println(pair.productElement(0)) //结果与  pair._1 相同
        println(pair.productElement(1)) //结果与  pair._2 相同
        
        println(pair.productArity)  //2 元数  相当于 list.size()
        
    }
}