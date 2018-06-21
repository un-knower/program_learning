package demo.spark.mllib.component

import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.linalg.Vectors

/**
 * 简单的权重更新 SimpleUpdater
 * 只有一个compute方法进行权重更新
 * 权重更新规则
 * weights=weights - stepSize/sqrt(iter)*gradient
 * 用步长除以迭代次数的平方根 作为本次迭代下降的因子
         返回本次梯度下降后更新的特征权重向量
 */

object SimpleUpdaterTest {
    def main(args: Array[String]): Unit = {
        val updater = new SimpleUpdater
        /**
         * weightsOld: Vector,
         * gradient: Vector,
      	 * stepSize: Double,
      	 * iter: Int,
         * regParam: Double)
         */
        val weightsOldArr = Array[Double](1,2,3,4)
        val weightsOld = Vectors.dense(weightsOldArr)  //上次迭代计算后的特征权重向量
        val gradient = Vectors.dense(Array[Double](2,3,4,5)) //本次迭代的特征权重向量
        val stepSize = 0.1    //步长
        val iter = 1 //迭代次数
        val regParam = 0.0 //正则参数，SimpleUpdater是一个简单的梯度下降方法，不包括正则化，所以该参数取值没有用到
        //compute方法
        //用步长除以迭代次数的平方根 作为本次迭代下降的因子
        //返回本次梯度下降后更新的特征权重向量
        val res = updater.compute(weightsOld, gradient, stepSize, iter, regParam) //更新权重
        println(res._1.toArray.mkString(",")) //0.8,1.7,2.6,3.5
        println(res._2) //0.0  这里源代码返回0
        
        
        var regVal = updater.compute(
      weightsOld, Vectors.dense(new Array[Double](weightsOld.size)), 0, 1, regParam)
          println(regVal._1)
          println(regVal._2)
          
          
          
          
      
    }
  
}