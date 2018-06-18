package demo.scala.study

//with 多个trait
//                    类             with trait
class Chinese extends Human with Animal{
  override def run(): Unit = {//如果trait中没有写run的方法体，则可以不写override也可以；如果重写一个非抽象的方法，必须写override
    println("run")
  }
}

object Chinese {
    def main(args: Array[String]): Unit = {
      val c = new Chinese
      c.run()
    }
}


