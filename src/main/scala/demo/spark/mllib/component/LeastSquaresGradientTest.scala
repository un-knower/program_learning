package demo.spark.mllib.component

import org.apache.spark.mllib.optimization.LeastSquaresGradient
import org.apache.spark.mllib.linalg.Vectors
/**
 * override def compute(data: Vector, label: Double, weights: Vector)
 * 方法返回两个值
 * 一、梯度值：x*(y-h⊙(x))
 * 二、损失值loss=1/2(y-h⊙(x))^2
 * 其中
 * x为损失值loss
 * y=data*weights 即为该权重下的数据结果值
 * h(⊙)=label 即为样本值
 * 
 * 
 * override def compute(data: Vector,label: Double,weights: Vector,
      cumGradient: Vector)
 * 方法只有一个返回值
 * 一、损失值
 * 跟上面的三个参数的compute方法相比，y计算公式变了
 * y=data*weights+cumGradient
 * 损失值的计算还是 loss = 1/2(y-h⊙(x))^2
 * 
 * 
 */
object LeastSquaresGradientTest {
    def main(args: Array[String]): Unit = {
        val gradient = new LeastSquaresGradient
        val dataArr =  Array[Double](1,2,3,4)
        val data =  Vectors.dense(dataArr)
        
        val weightsArr =  Array[Double](2,3,4,5)
        val weights =  Vectors.dense(weightsArr)
        val label = 43.0d
        val (da, loss) = gradient.compute(data, label, weights) //基于最小二乘计算梯度值和损失值
        println(da.toArray.mkString(",")) //-3.0,-6.0,-9.0,-12.0
        println(loss) //4.5
        
        val cumGradientArr = Array[Double](6,7,8,9) 
        val cumGradient = Vectors.dense(cumGradientArr)
        val loss2 = gradient.compute(data, label, weights, cumGradient)
        println(loss2) //4.5
        
    }
  
}