package demo.scala.list

object List_Thinking {

  def main(args: Array[String]) {
    val list : List[Int] = List(1,2,3,4,5)
    val listAny : List[Any] = list
    println(list.isEmpty)
    println(list.head)
    println(list.tail)
    println(list.length)
    println(list.drop(2))  // 删除前n个元素
    println(list.map(_*2))
    
    
  }

}