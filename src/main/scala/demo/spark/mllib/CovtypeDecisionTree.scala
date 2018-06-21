package demo.spark.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.RandomForest

/**
 * 决策树
 * 预测植被覆盖类型
 */
object CovtypeDecisionTree {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("").setMaster("local[4]")
        val sc = new SparkContext(conf)
        val rawData = sc.textFile("file/covtype.data")
        val data = rawData.map { line => 
            val values = line.split(",").map(_.toDouble)
            val featureVector = Vectors.dense(values.init) //前面是特征，最后一个目标
            val label = values.last-1 //决策树要求label从0开始，所以要减1
            LabeledPoint(label, featureVector)
        }
        
        //将数据划分
        
        val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8,0.1,0.1))
        trainData.cache //训练集
        cvData.cache //检验集  训练集和检验集用于为模型的超参数进行选择一个合适的值
        testData.cache //测试集
        
        val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), "gini", 4, 100)
        val metrics = getMetrics(model, data);
        //混沌矩阵
        println(metrics.confusionMatrix)
        //分类准确度
        metrics.precision  //分类准确度
        metrics.precision(1.0) //类别1的分类准确度
        
        metrics.recall  //召回
        metrics.recall(1.0) //类别1的召回
        
        
        (0 until 7).map{
            cat =>(metrics.precision(cat),metrics.recall(cat))
        }.foreach(println)
        
        
        val trainPriorProbabilities = classProbabilities(trainData)
        val cvPriorProbabilities = classProbabilities(cvData)
        val randomClassPrecision = trainPriorProbabilities.zip(cvPriorProbabilities).map{
            case(trainProb, cvProb)=>trainProb*cvProb
        }.sum
        
        println("randomClassPrecision:"+randomClassPrecision)
        
        /*决策树调优*/
        val evaluations = 
            for(impurity <- Array("gini","entropy");
                depth <- Array(1,20);
                bins <-Array(10,300))
                yield {
                    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int,Int](), impurity, depth, bins)
                    val predictionsAndLabel = cvData.map { example =>
                        (model.predict(example.features),example.label)
                    }
                    val accuray = new MulticlassMetrics(predictionsAndLabel)
                    ((impurity,depth,bins),accuray.precision)
                }
        evaluations.sortBy(_._2).reverse.foreach(println)
        
        
        /**同时使用训练集和验证集训练决策树*/
        val model3 = DecisionTree.trainClassifier(trainData.union(cvData), 7, Map[Int,Int](), "entropy", 20, 300)
        
        
        /*决策森林*/
        //训练数据，7个分类，指定类别特征10有4个取值，类别特征11有40个取值个数（即这些特征的桶的个数），20棵树，每层的评估特征选择策略为auto，使用熵，树的最大深度为30,300个桶
        val forest = RandomForest.trainClassifier(trainData, 7, Map(10->4, 11->40), 20, "auto", "entropy", 30, 300)
      
        /**进行预测*/
        
        val input = "2709,125,28,67,12,3224,253,207,61,6094,0,29"
        val vector = Vectors.dense(input.split(",").map(_.toDouble))
        forest.predict(vector)
        
        //程序的最后，要释放掉缓存的rdd
        trainData.unpersist()
        cvData.unpersist()
        testData.unpersist()
        
    }
    /**
     * 多分类评价指标
     */
    def getMetrics(model:DecisionTreeModel, data:RDD[LabeledPoint]):MulticlassMetrics= {
        val predictionsAndLabels = data.map { example => 
            (model.predict(example.features),example.label)    
        }
        new MulticlassMetrics(predictionsAndLabels)
    }
  
    /**
     * 每种类别所占的比例
     */
    def classProbabilities(data:RDD[LabeledPoint]):Array[Double] = {
        val countsByCategory = data.map(_.label).countByValue() //每种类别的个数
        //val countsByCategory = data.map(p=>(p.label , 1L)).reduceByKey(_+_).collectAsMap() //每种类别的个数
        val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
        counts.map(_.toDouble / counts.sum)
    }
    
    
    
    
    
}