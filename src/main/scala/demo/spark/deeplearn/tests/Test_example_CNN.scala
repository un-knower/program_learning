package demo.spark.deeplearn.tests

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import breeze.linalg.{CSCMatrix => BSM, DenseMatrix => BDM, DenseVector => BDV, Matrix => BM, SparseVector => BSV, Vector => BV, axpy => brzAxpy, max => Bmax, min => Bmin, sum => Bsum, svd => brzSvd}
import demo.spark.deeplearn.CNN.CNN

import scala.collection.mutable.ArrayBuffer

object Test_example_CNN {

  def main(args: Array[String]) {
    //1 构建Spark对象
    val conf = new SparkConf().setAppName("CNNtest")
    val sc = new SparkContext(conf)

    //2 测试数据
    val data_path = "/deeplearn/train_d3.txt"
    val examples = sc.textFile(data_path).cache()
    val train_d1 = examples.map { line =>
      val f1 = line.split("\t")
      val f = f1.map(f => f.toDouble)
      val y = f.slice(0, 10)
      val x = f.slice(10, f.length)
      (new BDM(1, y.length, y), (new BDM(1, x.length, x)).reshape(28, 28) / 255.0)
    }
    val train_d = train_d1.map(f => (f._1, f._2))
    
    //3 设置训练参数，建立模型
    // opts:迭代步长，迭代次数，交叉验证比例
    val opts = Array(50.0, 1.0, 0.0)
    train_d.cache
    val numExamples = train_d.count()
    println(s"numExamples = $numExamples.")
    val CNNmodel = new CNN().
      setMapsize(new BDM(1, 2, Array(28.0, 28.0))).
      setTypes(Array("i", "c", "s", "c", "s")).
      setLayer(5).
      setOnum(10).
      setOutputmaps(Array(0.0, 6.0, 0.0, 12.0, 0.0)).
      setKernelsize(Array(0.0, 5.0, 0.0, 5.0, 0.0)).
      setScale(Array(0.0, 0.0, 2.0, 0.0, 2.0)).
      setAlpha(1.0).
      CNNtrain(train_d, opts)

    //4 模型测试
    val CNNforecast = CNNmodel.predict(train_d)
    val CNNerror = CNNmodel.Loss(CNNforecast)
    println(s"NNerror = $CNNerror.")
    val printf1 = CNNforecast.map(f => (f.label.data,  f.predict_label.data)).take(200)
    println("预测值")
    for (i <- 0 until printf1.length) {
      val outi = printf1(i)._2.mkString("\t")
      println(outi)
    }

  }
}