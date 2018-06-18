package demo.scala.implicits

/*
隐式转换
特殊售票窗口（只接受特殊人群，比如学生、老人）
类型不对会尝试进行隐式转换
 */

case class SpecialPerson(val name:String)
case class Student(val name:String) 
case class Older(val name:String)
case class Teacher(val name:String)

object Implicit01 {
    
    
  implicit def object2SpecialPerson(obj:Object):SpecialPerson= { //Object比Any还大
      if(obj.getClass == classOf[Student]) {  //判断是否是该类型的两种方法，其一
          val stu = obj.asInstanceOf[Student]
          new SpecialPerson(stu.name)
      }else if(obj.isInstanceOf[Older]) {  //判断是否是该类型的两种方法，其二
          val older = obj.asInstanceOf[Older]
          new SpecialPerson(older.name)
      }else {
          Nil
      }
  }
  var ticketNumber = 0
  def buySpecialTicket(p:SpecialPerson)= {
      ticketNumber += 1 //票号+1
      "T-"+ticketNumber
  }
  
  def main(args: Array[String]): Unit = {
     val student = new Student("student")
     val older = new Student("older")
     val teacher = new Teacher("teacher")
     ticketNumber = 1
     val stu_buyRes = buySpecialTicket(student)  //当传入的参数不是该方法的类型时，会到当前上下文找所有的Implicit def定义的方法
     println(stu_buyRes)
     val older_buyRes = buySpecialTicket(older)
     println(older_buyRes)
     
     val tea_buyRes = buySpecialTicket(teacher)
     println(tea_buyRes)
     
         
      
     
  }
  
  
  
}