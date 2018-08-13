package demo.scala.string

object StringTest {
  def getZkAddress(zkAddress: String): String = zkAddress.split(",").map(_.split("\\/")(0)).mkString(",")

  def main(args:Array[String]): Unit = {
    println(getZkAddress("/test/c1,/test/c2"))

    println("/test/c1,/test/c2".split(",")(0).split("\\/").drop(1).mkString(":"))


  }
}
