package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo_apply {
    /**
     * 在Scala里面的class和object在Java层面里面合二为一，class里面的成员成了实例成员，object成员成了static成员
     */
    class Person(name:String,age:Int) {
        def apply() {
            println("class apply")
        }
    }
    object Person {
        def apply() {
            println("object apply")
        }
        def staticF() {
            println("staticF()")
        }
    } 
    def main(args: Array[String]): Unit = {
      val p = new Person("A",11)    //p:Person=Person@175a668   //实例化Person
 	  val p2 = Person()    //object apply P2:Unit=()            //调用object的apply方法
 	  val p3 = Person    //p3: Person.type = Person$@167593e    //静态Person

      //p 和  p2都没有staticF方法
      p3.staticF()
      
      Person.staticF()
    }
    
    
}