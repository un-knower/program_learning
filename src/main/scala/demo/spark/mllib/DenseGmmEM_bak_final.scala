//package demo.spark.mllib
//
//import org.apache.spark.{ SparkConf, SparkContext }
//import org.apache.spark.mllib.clustering.GaussianMixture
//import breeze.linalg.{diag, DenseMatrix => BreezeMatrix, DenseVector => BDV, Vector => BV}
//import org.apache.spark.mllib.linalg.{ Vectors, Vector }
//import org.apache.log4j.{ Level, Logger }
//import org.apache.spark.mllib.stat.distribution.MultivariateGaussian
//import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
//import org.apache.spark.rdd.RDD
//import scopt.OptionParser
//import org.apache.spark.mllib.clustering.GaussianMixtureModel
//import org.apache.spark.mllib.linalg.DenseMatrix
//import org.apache.spark.mllib.linalg.Matrices
//
///*
//
///usr/local/spark/spark-1.5.1/bin/spark-submit \
//--class com.mllib.demo.DenseGmmEM \
//--master spark://SparkMaster:7077 \
//--jars /data/spark/scopt_2.10-3.2.0.jar \
///data/spark/gmm.jar \
//-i /data/kmeans_data.txt -o /out
//
// */
//object DenseGmmEM_bak_final {
//    case class Params(
//        input: String = null,
//        inputTestLabel: String = null,
//        inputTest: String = null,
//        output: String = null,
//        k: Int = -1,
//        numIterations: Int = 30,
//        separator: String = ",")
//
//    def optionParse() = {
//
//        val defaultParams = Params() //默认参数
//        val parser = new OptionParser[Params]("DiDi_GMM") {
//            head("DiDi_GMM", "version 1.0") // adds usage text.
//
//            opt[String]('i', "input") //`-i value` or `--input value`.
//                .required() //Requires the option to appear at least once
//                .action((x, c) => c.copy(input = x)) ////Adds a callback function.
//                .text("input is a required input train file path property. default is " + defaultParams.input) //Adds description in the usage text.
//            opt[String]('l', "inputTestLabel") //`-i value` or `--input value`.
//                .action((x, c) => c.copy(inputTestLabel = x)) ////Adds a callback function.
//                .text("inputTestLabel is a optional input test file with label path property. default equals intput train file ") //Adds description in the usage text.
//            opt[String]('t', "inputTest") //`-i value` or `--input value`.
//                .action((x, c) => c.copy(inputTest = x)) ////Adds a callback function.
//                .text("inputTest is a optional input test file path property. default equals intput train file ") //Adds description in the usage text.
//
//            opt[String]('o', "output")
//                .action((x, c) => c.copy(output = x))
//                .text("output is a required result output path property. default is " + defaultParams.output)
//
//            opt[Int]('k', "k")
//                .action((x, c) => c.copy(k = x))
//                .text("k is a optional clusters num property. default is " + defaultParams.k)
//
//            opt[Int]('n', "numIterations")
//                .action((x, c) => c.copy(numIterations = x))
//                .text("k is a optional num of iteration property. default is " + defaultParams.numIterations)
//                .validate { x => //Adds custom validation
//                    if (x >= 1) success
//                    else failure("Value <numIteration> must be >= 1")
//                }
//            opt[String]('s', "separator")
//                .action((x, c) => c.copy(separator = x))
//                .text("separator is a optional property. default is " + defaultParams.separator)
//
//        }
//        parser
//
//    }
//    def gmmRun(params: Params) {
//        val defaultParams = Params() //默认参数
//        val inputTrainFile = params.input //训练集文件路径，带label
//        val inputTestFileWithLabel = params.inputTestLabel //测试集文件路径,该测试集带有label
//        val inputTestFile = params.inputTest //测试集文件路径
//        val output = params.output
//        val numIterations = params.numIterations
//        var k = params.k
//        val separator = params.separator
//
//        val conf = new SparkConf().setAppName("Gaussian Mixture Model EM").setMaster("local[4]")
//        val ctx = new SparkContext(conf)
//        //训练数据
//        val dataWithLabel = ctx.textFile(inputTrainFile).map {
//            line =>
//                val split = line.trim.split(separator)
//                (split.last, Vectors.dense(split.init.map(_.toDouble)))
//        }.cache()
//        //estimate the number of clusters by cross validation on the training
//        if (k == -1) {
//            k = CVClusters(dataWithLabel, numIterations,ctx) //得到适合的聚类个数k
//        }
//
//        //测试数据
//        if (inputTestFile != null ) {
//            val dataTest = ctx.textFile(inputTestFile).map {
//                line =>
//                    val split = line.trim.split(separator)
//                    Vectors.dense(split.map(_.toDouble))
//            }.cache()
//            run(dataWithLabel, dataTest, k, numIterations, true, output, ctx)
//        }
//        else if (inputTestFileWithLabel != null) {
//        	val testWithLabel = ctx.textFile(inputTestFileWithLabel).map {
//            line =>
//                val split = line.trim.split(separator)
//                (split.last, Vectors.dense(split.init.map(_.toDouble)))
//            }.cache()
//            run(dataWithLabel, testWithLabel, k, numIterations, true, output, ctx)
//        }else {
//        	run(dataWithLabel, dataWithLabel, k, numIterations, true, output, ctx)
//        }
//
//
//        ctx.stop()
//    }
//    def main(args: Array[String]): Unit = {
//        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
//        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
//
//        val parser = optionParse
//        val defaultParams = Params() //默认参数
//        parser.parse(args, defaultParams).map { params =>
//            gmmRun(params)
//        }.getOrElse {
//            println("error options")
//            sys.exit(1)
//        }
//
//    }
//
//    def CVClusters(data: RDD[(String, Vector)], maxIterations: Int,sc:SparkContext): Int = {
//
//        val dataWithIndex = data.sample(false, 1.0d).zipWithIndex().cache
//        val numInstances = dataWithIndex.count.toInt
//        val numFolds = if (numInstances < 10) numInstances else 10
//        var errorSum = -Double.MaxValue
//        var continueFlag = true
//        var k = 1
//        while (continueFlag) {
//            var templl = 0d
//            for (i <- 0 until numFolds) {
//                val cvTrain = trainCV(numFolds, i, dataWithIndex)
//                val cvTest = testCV(numFolds, i, dataWithIndex)
//
//                val tll = run(cvTrain, cvTest, k, maxIterations, false, null,sc)
//                templl += tll
//
//            } //for
//            println("CVLogLikely:"+templl+", k="+k)
//            if (errorSum < templl) {
//                k += 1
//                errorSum = templl
//
//            } else {
//                continueFlag = false
//            }
//        }
//        k -= 1
//        k
//    }
//
//
//    /**
//     * @param cvTrain 带标记的训练集
//     * @param cvTest 带标记的测试集
//     * @param k 聚类个数
//     * @param maxIterations 最大迭代次数
//     * @param verbose 是否显示聚类结果
//     * @return loglikelihood
//     */
//    private def run(cvTrain: RDD[(String, Vector)], cvTest: RDD[(String, Vector)], k: Int, maxIterations: Int, verbose: Boolean, output:String,sc:SparkContext): Double = {
//
//        val cvTrainData = cvTrain.map(_._2).cache()
//
//        val clusters = new GaussianMixture()
//            .setK(k)
//            .setMaxIterations(maxIterations)
//            .run(cvTrainData)
//
//        val prediction = clusters.predict(cvTest.map(_._2))
//        val predictionWithLabel = cvTest.zip(prediction)
//        if(output != null) {
//            predictionWithLabel.saveAsTextFile(output); //save to hdfs
//        }
//        if(verbose) {
//        	clusterPrecise(predictionWithLabel, k, verbose)
//        }
//
//        E(cvTrain, clusters, verbose, k, maxIterations, sc)
//
//    }
//    /**
//     * @param cvTrain 带标记的训练集
//     * @param cvTest 不带标记的测试集
//     * @param k 聚类个数
//     * @param maxIterations 最大迭代次数
//     * @param verbose 是否显示聚类结果
//     */
//    private def run(cvTrain: RDD[(String, Vector)], cvTest: RDD[Vector], k: Int, maxIterations: Int, verbose: Boolean, output:String,sc:SparkContext){
//
//    		val cvTrainData = cvTrain.map(_._2).cache()
//
//    				val clusters = new GaussianMixture()
//    		.setK(k)
//    		.setMaxIterations(maxIterations)
//    		.run(cvTrainData)
//
//    		val prediction = clusters.predict(cvTest)
//    		val predictionWithLabel = cvTest.zip(prediction)
//    		if(output != null) {
//    			predictionWithLabel.saveAsTextFile(output); //save to hdfs
//    		}
//    		clusterPrecise(predictionWithLabel, k, verbose)
//
//    }
//   //The E step of the EM algorithm. Estimate cluster membership probabilities.
//    def E(data: RDD[(String, Vector)],clusters:GaussianMixtureModel,verbose:Boolean,k:Int,maxIterations: Int,sc:SparkContext) ={
//        if(verbose) {
//            0.0d
//        }else {
//            val d = data.first()._2.size
//            val compute = sc.broadcast(ExpectationSum.add(clusters.weights, clusters.gaussians)_)
//            // aggregate the cluster contribution for all sample points
//            val sums = data.map(x => x._2).aggregate(ExpectationSum.zero(k, d))(compute.value, _ += _)
//            sums.logLikelihood // this is the freshly computed log-likelihood
//        }
//
//    }
//
//    /**
//     * 统计聚类准确信息
//     */
//    def clusterPrecise(predictionWithLabel: RDD[((String, Vector), Int)], k: Int, verbose: Boolean): Double = {
//        val data = predictionWithLabel.map(line => (line._1._1.toString, line._2)).cache()
//        val lev = 0
//        val numClusters = k
//        val trueCluster = data.map(_._1).distinct().count.toInt
//        val clusterTotals = new Array[Int](numClusters)
//        val clusterTotalsResult = data.map(x => (x._2, 1)).reduceByKey(_ + _).sortBy(_._1).collect
//        for (clusterTotal <- clusterTotalsResult) {
//            clusterTotals(clusterTotal._1.toInt) = clusterTotal._2
//        }
//
//        val sampleKV = data.map(x => (x, 1)).reduceByKey(_ + _).sortBy(_._1).collect
//        val counts = Array.ofDim[Int](numClusters, trueCluster)
//        var startClust = ""
//        var i = 0
//        sampleKV.foreach {
//            line =>
//                if (startClust == "") {
//                    startClust = line._1._1
//                } else if (startClust != line._1._1) {
//                    i += 1
//                    startClust = line._1._1
//                }
//                counts(line._1._2.toInt)(i) = line._2
//        }
//
//        val current = new Array[Double](numClusters + 1);
//        val best = new Array[Double](numClusters + 1);
//        best(numClusters) = Double.MaxValue
//        val error = 0;
//        //
//        mapClasses(numClusters, lev, counts, clusterTotals, current, best, error);
//
//        if (verbose) {
//            val labelsMap = data.map(_._1).distinct.sortBy(x => x).zipWithIndex.map(x => (x._2.toInt, x._1)).collectAsMap
//            val labels = collection.mutable.Map(labelsMap.toSeq: _*)
//            labels += ( -1 -> "No class")
//            val stringBuffer = new StringBuilder
//            stringBuffer.append("\n\n")
//
//             val eachLabelNum = scala.collection.mutable.Map[String, Int]()
//             var sumAll = 0
//             for (j <- 0 until counts(0).length) {
//                var sumEach = 0
//                for (i <- 0 until counts.length) {
//                    sumEach += counts(i)(j)
//                }
//                sumAll += sumEach
//                eachLabelNum += (labels(j) -> sumEach)
//            }
//
//            stringBuffer.append("Clustered Instances\n\n")
//            eachLabelNum.toList.sorted foreach {case(key,value) =>
//                stringBuffer.append(key+"\t\t"+value+"( "+value.toDouble/sumAll*100+"%)\n")
//            }
//
//            stringBuffer.append("\n\nClasses to Clusters:\n\n")
//            for (i <- 0 until numClusters) {
//                stringBuffer.append(i + "\t");
//            }
//            stringBuffer.append("<-- assigned to cluster\n")
//            for (j <- 0 until counts(0).length) {
//                for (i <- 0 until counts.length) {
//                    stringBuffer.append(counts(i)(j) + "\t")
//
//                }
//                stringBuffer.append("|" + labels(j) + "\n")
//            }
//            stringBuffer.append("\n")
//
//            for (i <- 0 until numClusters) {
//                stringBuffer.append("Cluster " + i + " <-- " + labels(best(i).toInt) + "\n")
//            }
//            stringBuffer.append("\nIncorrectly clustered instances : " + best(best.length - 1) + " " + best(best.length - 1) / data.count.toDouble * 100 + "%");
//            println(stringBuffer.toString)
//        }
//        best(best.length - 1) //return error
//
//    }
//    /**
//     * 统计测试集不带标签聚类信息
//     */
//    def clusterPrecise(predictionWithLabel: RDD[(Vector, Int)], k: Int, verbose: Boolean) = {
//        if(verbose) {
//            val sampleKV = predictionWithLabel.map(x=>(x._2, 1)).reduceByKey(_+_)
//    	    val stringBuffer = new StringBuilder
//    	    stringBuffer.append("\n\n")
//    	    stringBuffer.append("Predict Instances\n\n")
//    	    val sumAll = predictionWithLabel.count()
//    	    sampleKV.collect.foreach{line =>
//    	        stringBuffer.append(line._1+"\t\t"+line._2+"( "+line._2.toDouble/sumAll*100+"%)\n")
//    	    }
//            stringBuffer.append("\n\n")
//            println(stringBuffer.toString)
//        }
//    }
//
//    def trainCV(numFolds: Int, numFold: Int, dataWithIndex: RDD[((String, Vector), Long)]): RDD[(String, Vector)] = {
//        val numInstances = dataWithIndex.count.toInt
//        //require(numFolds < 2, "Number of folds must be at least 2!")
//        //require(numFolds > numInstances, "Can't have more folds than instances!")
//        var numInstForFold = numInstances / numFolds
//        var offset = 0
//        if (numFold < numInstances % numFolds) {
//            numInstForFold += 1
//            offset = numFold
//        } else {
//            offset = numInstances % numFolds
//        }
//
//        val first = numFold * (numInstances / numFolds) + offset
//        val trainPart1 = dataWithIndex.filter(x => x._2 >= 0 && x._2 < first)
//        val trainPart2 = dataWithIndex.filter(x => x._2 >= (first + numInstForFold) && x._2 < numInstances)
//
//        val trainWithIndex = trainPart1.union(trainPart2)
//        val train = trainWithIndex.map(x => x._1)
//        train
//    }
//
//    def testCV(numFolds: Int, numFold: Int, dataWithIndex: RDD[((String, Vector), Long)]): RDD[(String, Vector)] = {
//        val numInstances = dataWithIndex.count.toInt
//        //        require(numFolds < 2, "Number of folds must be at least 2!")
//        //        require(numFolds > numInstances, "Can't have more folds than instances!")
//        var numInstForFold = numInstances / numFolds
//        var offset = 0
//        if (numFold < numInstances % numFolds) {
//            numInstForFold += 1
//            offset = numFold
//        } else {
//            offset = numInstances % numFolds
//        }
//
//        val first = numFold * (numInstances / numFolds) + offset
//        val testPart = dataWithIndex.filter(x => x._2 >= first && x._2 < numInstForFold + first)
//        val test = testPart.map(x => x._1)
//        test
//
//    }
//
//    /**
//     * Finds the minimum error mapping of classes to clusters. Recursively
//     * considers all possible class to cluster assignments.
//     *
//     * @param numClusters
//     * the number of clusters
//     * @param lev
//     * the cluster being processed
//     * @param counts
//     * the counts of classes in clusters
//     * @param clusterTotals
//     * the total number of examples in each cluster
//     * @param current
//     * the current path through the class to cluster assignment tree
//     * @param best
//     * the best assignment path seen
//     * @param error
//     * accumulates the error for a particular path
//     */
//    def mapClasses(numClusters: Int, lev: Int, counts: Array[Array[Int]],
//                   clusterTotals: Array[Int], current: Array[Double], best: Array[Double], error: Int) {
//        // leaf
//        if (lev == numClusters) {
//            if (error < best(numClusters)) {
//                best(numClusters) = error;
//                for (i <- 0 until numClusters) {
//                    best(i) = current(i);
//                }
//            }
//        } else {
//            // empty cluster -- ignore
//            if (clusterTotals(lev) == 0) {
//                current(lev) = -1; // cluster ignored
//                mapClasses(numClusters, lev + 1, counts, clusterTotals,
//                    current, best, error);
//            } else {
//                // first try no class assignment to this cluster
//                current(lev) = -1; // cluster assigned no class (ie all errors)
//                mapClasses(numClusters, lev + 1, counts, clusterTotals,
//                    current, best, error + clusterTotals(lev));
//                // now loop through the classes in this cluster
//                for (i <- 0 until counts(0).length) {
//                    if (counts(lev)(i) > 0) {
//                        var ok = true;
//                        // check to see if this class has already been assigned
//                        for (j <- 0 until lev if (ok)) {
//                            if (current(j).toInt == i) {
//                                ok = false;
//                            }
//                        }
//                        if (ok) {
//                            current(lev) = i;
//                            mapClasses(
//                                numClusters,
//                                lev + 1,
//                                counts,
//                                clusterTotals,
//                                current,
//                                best,
//                                (error + (clusterTotals(lev) - counts(lev)(i))));
//                        }
//                    }
//                }
//            }
//        }
//
//    } //function mapClasses
//}
//
//
////private object ExpectationSum {
////  def zero(k: Int, d: Int): ExpectationSum = {
////    new ExpectationSum(0.0, Array.fill(k)(0.0),
////      Array.fill(k)(BDV.zeros(d)), Array.fill(k)(BreezeMatrix.zeros(d,d)))
////  }
////
////  // compute cluster contributions for each input point
////  // (U, T) => U for aggregation
////  def add(
////      weights: Array[Double],
////      dists: Array[MultivariateGaussian])
////      (sums: ExpectationSum, x: Vector): ExpectationSum = {
////      lazy val EPSILON = {
////    var eps = 1.0
////    while ((1.0 + (eps / 2.0)) != 1.0) {
////      eps /= 2.0
////    }
////    eps
////  }
////    val p = weights.zip(dists).map {
////      case (weight, dist) => EPSILON + weight * dist.pdf(x)
////    }
////    val pSum = p.sum
////    sums.logLikelihood += math.log(pSum)
////    sums
////  }
////}
////// Aggregation class for partial expectation results
////private class ExpectationSum(
////    var logLikelihood: Double,
////    val weights: Array[Double],
////    val means: Array[BDV[Double]],
////    val sigmas: Array[BreezeMatrix[Double]]) extends Serializable {
////
////  val k = weights.length
////
////  def +=(x: ExpectationSum): ExpectationSum = {
////    var i = 0
////    while (i < k) {
////      weights(i) += x.weights(i)
////      means(i) += x.means(i)
////      sigmas(i) += x.sigmas(i)
////      i = i + 1
////    }
////    logLikelihood += x.logLikelihood
////    this
////  }
////}
//
