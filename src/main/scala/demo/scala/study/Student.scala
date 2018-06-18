package demo.scala.study

/**
 * 每个类都有主构造器，主构造器的参数直接放置类名后面，与类交织在一起
 * 跟在类后面的叫主构造器
 */

//不写val或者var，意味着private[this] val gender: String，其伴生对象无法访问
//有默认初始值的属性，在new时不传参也可以
//如果是val age:Int = 18 则在new时可以更改值比如val s = new Student("123","abc", "male",20)，但是不可以s.age = 20
class Student(val id:String, var name:String ,gender:String, var age:Int = 18){
  
}



object Student {
    def main(args: Array[String]): Unit = {
        val s = new Student("123","abc", "male")
        println(s.id)
        println(s.name)       
        
        s.name = "dbc"  //修改
        println(s.name)       
        
        //s.gender = "" 不能访问
        
        
        
        
        
        
    }
}