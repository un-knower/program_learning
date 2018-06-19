package demo.scala.study

/**
 * 
 */
object ScakaDemo22 {
    def main(args: Array[String]): Unit = {
        /**
         * == 已经被仔细的加工过了
         * 比较规则：首先检查左侧是否为null，如果不是，调用equals方法。
         * 由于equals是一个方法，因此比较的精度取决于左边的参数。又由于已经有自动的null检查，因此不需要手动再检查一次了。
         *
         * ==在java中比较引用是否相同，scala与之对应的是eq，反义词是ne 
         * 
         */
        println(1==2)    //false
        println(List(1,2,3) == List(1,2,3))    //true
        println(List(1,2,3) == null)    //false
        println(null == List(1,2,3))    //false
        
                
        
    }
  
}