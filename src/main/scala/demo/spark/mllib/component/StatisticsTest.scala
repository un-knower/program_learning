package demo.spark.mllib.component

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary

/**
 * 矩阵统计信息 Statistics
 */
object StatisticsTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val observations = sc.parallelize(Seq(
        Vectors.dense(1.0,10.0,100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
    ))
    
    //Compute column summary statistics
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.mean)  //[2.0,20.0,200.0] a dense vector containing the mean value for each column
    println(summary.variance)  //[1.0,100.0,10000.0] column-wise variance
    println(summary.numNonzeros)  //[3.0,3.0,3.0] number of nonzeros in each column
    println(summary.max)
    println(summary.min) //[1.0,10.0,100.0]
    
    println(summary.normL1)  //[6.0,60.0,600.0]
    println(summary.normL2) //[3.7416573867739413,37.416573867739416,374.16573867739413]
    
    println(summary.numNonzeros)  //[3.0,3.0,3.0]
    
    
    
   
    
    
  }
}