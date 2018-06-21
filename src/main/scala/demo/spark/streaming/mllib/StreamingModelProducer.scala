package demo.spark.streaming.mllib

import scala.util.Random
import java.net.ServerSocket
import java.io.PrintWriter

/**
 * 使用Spark的流处理结合MLlib的基于SGD的在线学习能力，可以创建实时的机器学习模型
 * 当数据流到达时实时更新学习模型
 * 
 * Spark在StreamingLinearAlgorithm类中提供了内建的流式机器学习模型。当前只实现了线性回归
 */

/**
 * 随机线性回归数据的生成器
 */
object StreamingModelProducer {
    import breeze.linalg._
    def main(args: Array[String]): Unit = {
        val MaxEvents = 100   //每秒处理活动的最大数据
        val NumFeatures = 100     //每秒处理活动的特征向量的特征数量
        val random = new Random()
        
        /**生成服从正太分布的稠密向量的函数*/
        def generateRandomArray(n:Int)=Array.tabulate(n)(_=>
            random.nextGaussian())
        
        //生成一个确定的随机模型权重向量
        val w = new DenseVector(generateRandomArray(NumFeatures)) //某一个方向
        val intercept = random.nextGaussian()*10 //在该方向上的偏移
        
        /**生成确定的数据点，通过计算已知向量及随机特征点积并加上偏移后的值对应的目标值*/
        def generateNoisyData(n:Int) = {
            (1 to n).map{i=>
                val x = new DenseVector(generateRandomArray(NumFeatures))
                val y:Double = w.dot(x)
                val noisy = y+intercept
                (noisy,x)
            }
        }
        
        //最后，使用和之前生成器类似的代码来初始化一个网络连接，并以文本形式每秒发送随机数据量（0-100之间）的数据点
        
        val listener = new ServerSocket(9999)
        println("listening on port:9999")
        
        while(true){
            val socket = listener.accept();
            new Thread(){
                override def run = {
                    println("Got client connected from: "+socket.getInetAddress)                    
                    val out = new PrintWriter(socket.getOutputStream, true)
                    while(true) {
                        Thread.sleep(1000)
                        val num = random.nextInt(MaxEvents)
                        val data = generateNoisyData(num)
                        data.foreach{ case(y,x)=>
                            val xStr = x.data.mkString(",")
                            val eventStr = s"$y\t$xStr"   //数值\t特征值
                            out.write(eventStr)
                            out.write("\n")
                        }
                        out.flush()
                        println(s"Created $num events...")
                    }
                }//run
                
            }.start()//Thread
            
        }//while
        
        
        
              
    }
    
  
}