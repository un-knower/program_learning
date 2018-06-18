package demo.scala.study

object ScalaDemo_Map {
    def main(args:Array[String]):Unit= {
        val treasureMap = scala.collection.mutable.Map[Int, String]() //可变Map
        treasureMap += (1->"Go to islan")
        treasureMap += (2->"Find big X on ground")
        treasureMap += (3->"Dig")
        println(treasureMap)  //Map(2 -> Find big X on ground, 1 -> Go to islan, 3 -> Dig)
        
        val immutableMap = Map( //默认是不可变Map
            1 -> "a", 2-> "b", 3-> "c"        
        )
        println(immutableMap(3)) //c
        
        
        /*map的遍历
         * map每个元素的组成相当于是一个元组
         * */
        treasureMap.foreach(kv=>println(kv._1+":"+kv._2))
        treasureMap.foreach(kv=>println(kv._1+":"+kv._2))
        
        println("map sort by key")
        /*对Map中的Key或Value进行排序*/
        treasureMap.toList.sorted foreach {
            case(key,value) =>
                println(key+":"+value)
        }
        
        /*对Map中的Key或Value进行排序*/
        println("map sort by value")
        treasureMap.toList.sortBy(_._2) foreach {
        case(key,value) =>
        println(key+":"+value)
        }
        
        
        /*convert immutable.Map to mutable.Map*/
        println("convert immutable.Map to mutable.Map")
        val mutableMap = collection.mutable.Map(immutableMap.toSeq: _*)
        mutableMap += (4->"d")
        println(mutableMap) 
        
    }
}