package demo.spark.mllib.component

import org.apache.spark.mllib.linalg.{Vectors, Vector}

object VectorTest {
  def main(args:Array[String]) = {
    val vd:Vector = Vectors.dense(2, 0, 6)
    println(vd(2))
    
    val vs:Vector = Vectors.sparse(5, Array(0,1,2,3), Array(9,5,2,7))
    println(vs(2))
    
    val vs2:Vector = Vectors.sparse(5, Array(1,3,0,2), Array(9,5,2,7))
    println(vs2(3))
    
  }
}