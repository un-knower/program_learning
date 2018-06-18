package demo.scala.study

//伴生类
/*对属性的访问权限*/
class People {
    val id = "9527"   //getId 没有setId   val修饰的相当于final
    
    var name = "华安"  //getName setName
    
    private var gender:String= "male"  //只能在伴生对象中访问到
    
    private[this] var pop:String = _ //未被初始化，根据其类型在new时初始化  //[this]只能在定义的该类中被访问，在伴生对象中也不能访问到
    
    def printPop:Unit = {
        println(pop)
    }
    
    
    
}


//伴生对象
object People { 
    def main(args: Array[String]): Unit = {
        val p = new People
        println(p.id+","+p.name)
      
        println(p.gender)   //只能在伴生对象中访问到
        p.gender = "男"  //可以修改
        println(p.gender)
        
        
        //p.pop // 在伴生对象中也不能访问到
        p.printPop
        
        
        val g = new Gril
        
        //val g2 = new Gril2 报错
        
    }
}


/*对类的访问权限*/
//包访问权限，这个类只能在demo.scala.study包或子包中被访问
private[study] class Gril {
    
}



//后面的private：私有的构造方法，该类只能在其伴生对象中被new
private[study]  class Gril2 private {
    
}





















