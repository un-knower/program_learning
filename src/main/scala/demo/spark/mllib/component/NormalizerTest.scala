package demo.spark.mllib.component

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.Normalizer
/**
 * L^p normal
 * http://spark.apache.org/docs/latest/mllib-feature-extraction.html#normalizer
 */
object NormalizerTest {
   def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val observations = sc.parallelize(Seq(
        Vectors.dense(1.0,10.0,100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
    ))
    
    val normalizer1 = new Normalizer() // L2
    val normalizer2 = new Normalizer(p=Double.PositiveInfinity)
    
    val data1 = observations.map {x=> normalizer1.transform(x) }
    data1.collect.foreach(println)
    
//[1/math.sqrt(1*1+10*10+100*100),10/math.sqrt(1*1+10*10+100*100),100/math.sqrt(1*1+10*10+100*100)]   
/*    
[0.009949879346007117,0.09949879346007116,0.9949879346007117]   
[0.009949879346007117,0.09949879346007116,0.9949879346007117]
[0.009949879346007115,0.09949879346007116,0.9949879346007116]   
  */
    
    val data2 = observations.map {x=> normalizer2.transform(x) }
    data2.collect.foreach(println)
/*    
[0.01,0.1,1.0]
[0.01,0.1,1.0]
[0.01,0.1,1.0]    
  */  
  }
}