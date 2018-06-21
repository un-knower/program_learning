package demo.spark.streaming.mllib

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import breeze.linalg.DenseVector
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * 流回归模型
 */
object SimpleStreamingModel {
    def main(args: Array[String]): Unit = {
        
        val ssc = new StreamingContext("local[2]","app name", Seconds(10))
        val stream = ssc.socketTextStream("localhost", 9999)
        
        //建立大量的特征来匹配输入的流数据记录的特征
        //建立一个零向量来作为流回归模型的初始权值向量
        val NumFeatures = 100
        val zeroVector:DenseVector[Double] = DenseVector.zeros[Double](NumFeatures) 
        val zeroVectorData:Array[Double] = zeroVector.data
        //训练两个模型
        val model1 = new StreamingLinearRegressionWithSGD()
                        .setInitialWeights(Vectors.dense(zeroVectorData))
                        .setNumIterations(1)
                        .setStepSize(0.01)
        val model2 = new StreamingLinearRegressionWithSGD()
                        .setInitialWeights(Vectors.dense(zeroVectorData))
                        .setNumIterations(1)
                        .setStepSize(1.0)
        
        //创建一个标签点的流
        val labeledStream = stream.map{ event=>
            val split = event.split("\t")
            val y = split(0).toDouble
            val features = split(1).split(",").map(_.toDouble)
            LabeledPoint(label=y, features=Vectors.dense(features))
        }
                        
        //在流上训练测试模型，并打印预测结果作为展示
        model1.trainOn(labeledStream)
        model2.trainOn(labeledStream)
//        model1.predictOn(labeledStream.map(_.features)).print()
//        model2.predictOn(labeledStream.map(_.features)).print()
        
        //使用转换算子创建包含模型错误率的流
        val predsAndTrue = labeledStream.transform{rdd=>   //对 DStream 里的 RDD 做复杂的转换操作的，这就是 transform 方法
            val latest1 = model1.latestModel()
            val latest2 = model2.latestModel()
            rdd.map { point =>  
                val pred1 = latest1.predict(point.features)    
                val pred2 = latest2.predict(point.features)
                (pred1-point.label, pred2-point.label)
            }
        }


        //对于每个模型的每个批次，输出MSE和RMSE统计结果
        predsAndTrue.foreachRDD{(rdd,time) =>
            val mse1 = rdd.map{case(err1,err2)=>err1*err1}.mean
            val rmse1 = math.sqrt(mse1)
            val mse2 = rdd.map{case(err1,err2)=>err2*err2}.mean
            val rmse2 = math.sqrt(mse2)
            
            println(s"""
              |-------------------------------------------
              |Time: $time
              |-------------------------------------------
              """.stripMargin)
            println(s"MSE current batch: Model 1: $mse1 ; Model2: $mse2")  
            println(s"RMSE current batch: Model 1: $rmse1 ; Model2: $rmse2")
            println("...\n")
        }
        
        
        
        
        
        ssc.start()
        ssc.awaitTermination()
      
    }
  
}