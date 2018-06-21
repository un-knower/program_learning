package demo.spark.mllib.component

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.log4j.{ Level, Logger }
import scala.reflect.ClassTag

object SampleTest {
    def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
        val conf = new SparkConf().setAppName("sample").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val data = sc.parallelize(Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j","k","l"))
        val dataWithIndex = data.zipWithIndex().cache
        CVClusters(data)

    }
    def CVClusters[U:ClassTag](data: RDD[U]) {
        
    	val dataWithIndex = data.zipWithIndex().cache
        val numInstances = dataWithIndex.count.toInt
        val numFolds = if (numInstances < 10) numInstances else 10
        for (i <- 0 until numFolds) {
            val cvTrain = trainCV(numFolds, i, dataWithIndex)
       		val cvTest = testCV(numFolds,i, dataWithIndex)
            println(i+"::cvTrain::::")
            cvTrain.foreach(println)
            println(i+"::cvTest::::")
            cvTest.foreach(println)
        }

    }

    def trainCV[U:ClassTag](numFolds: Int, numFold: Int, dataWithIndex: RDD[(U, Long)]): RDD[U] = {
        val numInstances = dataWithIndex.count.toInt
        //require(numFolds < 2, "Number of folds must be at least 2!")
        //require(numFolds > numInstances, "Can't have more folds than instances!")
        var numInstForFold = numInstances / numFolds
        var offset = 0
        if (numFold < numInstances % numFolds) {
            numInstForFold += 1
            offset = numFold
        } else {
            offset = numInstances % numFolds
        }

        val first = numFold * (numInstances / numFolds) + offset
        val trainPart1 = dataWithIndex.filter(x => x._2 >= 0 && x._2 < first)
        val trainPart2 = dataWithIndex.filter(x => x._2 >= (first + numInstForFold) && x._2 < numInstances )

        val trainWithIndex = trainPart1.union(trainPart2)
        val train = trainWithIndex.map(x=>x._1)
        train
    }
    def testCV[U:ClassTag](numFolds: Int, numFold: Int, dataWithIndex: RDD[(U, Long)]): RDD[U] = {
        val numInstances = dataWithIndex.count.toInt
//        require(numFolds < 2, "Number of folds must be at least 2!")
//        require(numFolds > numInstances, "Can't have more folds than instances!")
        var numInstForFold = numInstances / numFolds
        var offset = 0
        if (numFold < numInstances % numFolds) {
            numInstForFold += 1
            offset = numFold
        } else {
            offset = numInstances % numFolds
        }

        val first = numFold * (numInstances / numFolds) + offset
        val testPart = dataWithIndex.filter(x => x._2 >= first && x._2 < numInstForFold+first)
        val test = testPart.map(x=>x._1)
        test

    }

}