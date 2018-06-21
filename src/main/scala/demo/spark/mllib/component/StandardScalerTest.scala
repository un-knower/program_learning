package demo.spark.mllib.component

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.StandardScaler

/**
 * 正则化
 * http://spark.apache.org/docs/latest/mllib-feature-extraction.html#standardscaler
 */
object StandardScalerTest {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val observations = sc.parallelize(Seq(
        Vectors.dense(1.0,10.0,100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
    ))
    
    val scaler1 = new StandardScaler().fit(observations) //默认withMean=false, withStd=true
    val scaleredData1 = scaler1.transform(observations)
    scaleredData1.collect().foreach { println }
    /**
[1.0,1.0,1.0]
[2.0,2.0,2.0]
[3.0,3.0,3.0]
     */
    
    val scaler2 = new StandardScaler(withMean=false, withStd=true).fit(observations)
    val scaleredData2 = scaler2.transform(observations)
    scaleredData2.collect().foreach { println }
/*
[1.0,1.0,1.0]
[2.0,2.0,2.0]
[3.0,3.0,3.0]*/   
    
    val scaler3 = new StandardScaler(withMean=true, withStd=false).fit(observations)
    val scaleredData3 = scaler3.transform(observations)
    scaleredData3.collect().foreach { println }
    /*
[-1.0,-10.0,-100.0]
[0.0,0.0,0.0]
[1.0,10.0,100.0]    
    */
    
    val scaler4 = new StandardScaler(withMean=false, withStd=false).fit(observations)
    val scaleredData4 = scaler4.transform(observations)
    scaleredData4.collect().foreach { println }
/*
[1.0,10.0,100.0]
[2.0,20.0,200.0]
[3.0,30.0,300.0]
  */
    val scaler5 = new StandardScaler(withMean=true, withStd=true).fit(observations)
    val scaleredData5 = scaler5.transform(observations)
    scaleredData5.collect().foreach { println }
    /*
[-1.0,-1.0,-1.0]
[0.0,0.0,0.0]
[1.0,1.0,1.0]   
* */

  }

}