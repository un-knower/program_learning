package demo.scala.study

/**
 * @author guangliang
 */
object ScalaDemo9 {
	/**
	 * 特质
     * 继承多类怎么办？
     * java采取了非常强的限制策略。类智能拓展自一个超类：它可以实现任意数量的接口，但接口只能包含抽象方法，而不能包含字段
     * scala提供”特质“而非接口。
     * 特质可以同时拥有抽象方法和具体方法，而类可以实现多个特质
     *      
	 */
    def main(args: Array[String]): Unit = {
        trait Logger {
           def log(msg:String) //这是个抽象方法。不需要将方法声明为abstract--特质中未被实现的方法默认就是抽象方法
        }
        
        class ConsoleLogger extends Logger { //用extends 而不是implements
            def log(msg:String) { //不需要写override
                println(msg)
            }
        }
        
        /**
         * 如果需要的特质不止一个，可以用with关键字来添加额外的特质
         * 这里使用的是java类库的Cloneable，Serializable
         * 素有java接口都可以作为Scala的特质使用
         * 
         */
        abstract class ConsoleLogger2 extends Logger with Cloneable with Serializable
        
        /**
         * 特质也可以有具体的方法
         * 继承该trait的类，相当于增加（混入）了一个新的方法log(msg:String)
         */
        
        trait Logger2 {
            def log(msg:String) {
                println(msg)
            }
        }
        
        /**
         * 一个类继承多个特质，一般来说，特质从最后一个开始被处理
         */
        
        /**
         * apply方法接收构造参数，然后将它们变成对象
         * 而unapply方法接收一个对象，然后从中提取值--通常这些值就是当初用来构造该对象的值
         */
        
        object Name{
            def unapply(input:String)= {
                val pos = input.indexOf(" ")
                if(pos == -1) Some(None,None)
                else Some((input.substring(0, pos),input.substring(pos+1)))
            }
        }
        
        val author = "Cay Horstmann"
    	val author2 = "CayHorstmann"
        val Name(first,last) = author
        val Name(first2,last2) = author2
        println(first)    //Cay
        println(last)    //Horstmann
        println(first2)    //None
        println(last2)    //None
        /**
         * 带单个参数或无参的提取器
         * 在scala中，并没有只带一个组件的元组，如果unapply方法要提取单值，
         * 则它应该返回一个目标类型的Option
         */
        object Number {
            def unapply(input:String):Option[Int] = {
                try {
                    Some(Integer.parseInt(input))
                }catch {
                  case ex: NumberFormatException => None // TODO: handle error
                }
            }
        }
        
        val Number(n) = "1729"
        println(n)    //1729
        
        /**
         * 要提取任意长度的序列，我们应该用unapplySeq来命名方法
         * 返回一个Option[Seq[A]] ，其中A是被提取的值的类型
         */
        object Name2 {
            def unapplySeq(input:String):Option[Seq[String]] ={
                if(input.trim=="") None
                else
                    Some(input.trim.split("\\s+"))
            }
        }
        
        
        
        
        
        
        
        
        
    }    
    
    
    
    
    
    
}