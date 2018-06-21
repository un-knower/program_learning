package demo.spark.mllib

import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import java.util.Random

/**
 * Created with IntelliJ IDEA.
 * User: qingjian
 * Date: 16-5-30
 * Time: 下午4:00
 * 音乐推荐
 *
 **/
object ProfileALS {
  def main(args:Array[String]):Unit= {
    val conf = new SparkConf().setAppName("").setMaster("local[4]")
    val sc = new SparkContext(conf)
    /*读取用户播放艺术家次数文件*/
    val rawUserArtistData = sc.textFile("file/user_artist_data.txt")
    //该文件有三列，分别为用户Id，艺术家ID和播放次数，空格分隔
//1000002 1 55
//1000002 1000006 33
//1000002 1000007 8
//1000002 1000009 144
//1000002 1000010 314
//1000002 1000013 8

    //显示用户统计信息,用于查看用户id是否能用Int类型表示，发现远小于2147483647
    println(rawUserArtistData.map(_.split(" ")(0).toDouble).stats())
    //显示艺术家统计信息,用于查看id是否能用Int类型表示，发现远小于2147483647
    println(rawUserArtistData.map(_.split(" ")(1).toDouble).stats())
    
    
    /*读取艺术家ID和对应名称文件*/
    val rawArtistData = sc.textFile("file/artist_data.txt")
    //该文件有两列，分别为艺术家ID和艺术家名称
//    val artistById3 = rawArtistData.map { line =>  
//        val (id, name) = line.span(_!='\t') //用第一个\t进行分割成两部分，
//        (id.toInt, name.trim)
//    } 
    //文件中有少量行看起来是非法的：有些行没有制表符，有些行不小心加入了换行符，这将导致NumberFormatException，他们不应该有输出结果
    //但是map函数要求每个输入必须严格返回一个值。
    //①可以使用filter删除那些无法解析的行，但是会重复解析逻辑
    //②当需要将每个输入映射为零个或多个时，应该使用flatMap。配合使用Some
    
    val artistById = rawArtistData.flatMap { line => 
        val (id, name) = line.span(_ != '\t')
        if(name.isEmpty) {
            None
        }else {
            try {
            Some(
              (id.toInt, name.trim)
                )
            } catch{
                case e:NumberFormatException => None
            }    
        }
    }
    println(artistById.count)
    //展示一部分数据
    val artistById1 = artistById.take(5)
    println("id:"+artistById1(0)._1)
    println("name:"+artistById1(0)._2)
    
    
    /*读取艺术家别名文件*/
    val rawArtistAlias = sc.textFile("file/artist_alias.txt")
    //该文件有两列，有部分行只有一列，需要过滤掉，第一列是不良id，第二列是需要映射的良好id
    val artistAlias = rawArtistAlias.flatMap { line => 
        val tokens = line.split("\t")    
        if(tokens(0).isEmpty()){ //如果只有一列
            None
        }
        else {
            Some(
                    (tokens(0).toInt,tokens(1).toInt)
                 )
        }
    }.collectAsMap() //保存为Map结构（字典），以供方便调用
    //有条记录是 6803336 1000010 即将6803336映射为1000010，那么看下这两条对应的艺术家名称是什么
    //即需要将    Aerosmith (unplugged) 修改为   Aerosmith
    println("6803336:"+artistById.lookup(6803336)) //6803336:WrappedArray(Aerosmith (unplugged))
    println("1000010:"+artistById.lookup(1000010)) //1000010:WrappedArray(Aerosmith) 
    
    
    /*需要将转化字典广播出去*/
    val bArtistAlias = sc.broadcast(artistAlias)
    
    val trainData = rawUserArtistData.map { line =>
        val Array(userID, artistID, count) = line.split(" ").map(_.toInt)
        val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID) //进行转换
        Rating(userID, finalArtistID, count) //Rating对象是ALS算法实现对“用户-产品-值”的抽象，这里产品用艺术家id，评分使用点播次数    
    }.cache //因为ALS算法是迭代的，如果不调用cache，那么每次要用到RDD时都要从原始数据中重新计算，所以这里cache一下
    
    /*训练ALS模型*/
    //                            数据               隐含矩阵k 迭代次数 正则因子，推荐为0.01 置信度
    val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)
    //模型会训练处两个不同的RDD，分别表示 用户-特征 和 产品-特征 这两个大型矩阵
    println( model.userFeatures.mapValues { _.mkString(";") }.first )
    println( model.productFeatures.mapValues { _.mkString(";") }.first )
        
    
    /*检测推荐效果*/
    //查找用户2093760 听过的艺术家
    val rawArtistsIdForUser2093760 = rawUserArtistData.map(_.split(" "))
                                             .filter{case Array(user,_,_)=>user.toInt == 2093760}
                                             .map{case Array(_, artist, _)=>artist.toInt}
                                             .collect.toSeq
    val rawArtistsNameForUser2093760 = artistById.filter{ case(artistid,name)=>
        rawArtistsIdForUser2093760.contains(artistid)
                                                 
    }.values.collect.foreach(println)
//David Gray
//Blackalicious
//Jurassic 5
//The Saw Doctors
//Xzibit   
    
    //模型推荐5个
    val recommendations = model.recommendProducts(2093760, 5)
    recommendations.foreach(println)
    
//Rating(2093760,1001819,0.027127922017740027)
//Rating(2093760,2814,0.02682038740669134)
//Rating(2093760,1300642,0.02667553793326981)
//Rating(2093760,4605,0.026531516623781577)
//Rating(2093760,1811,0.02633274039855785)
    
    //将这个5个艺术家的名字找出来
    artistById.filter{ case(id, name)=>
        recommendations.map(_.product).contains(id)     
    }.values.collect().foreach(println)
//50 Cent
//Snoop Dogg
//Dr. Dre
//2Pac
//The Game   
    
    
    /*评价推荐质量*/
                                         
    val allData = buildRatings(rawUserArtistData, bArtistAlias)
    val Array(trainData2, cvData) = allData.randomSplit(Array(0.9,0.1))  //切分数据
    trainData2.cache()
    cvData.cache()
    
    val allItemIds = allData.map(_.product).distinct.collect()
    val bAllItemIDs = sc.broadcast(allItemIds)
    
    val model2 = ALS.trainImplicit(trainData2, 10, 5, 0.01, 1.0)
    val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
    println("auc:"+auc)
    
    //将播放次数推荐给用户的模型的auc
    val auc2 = areaUnderCurve(cvData, bAllItemIDs, predictMostListened(sc, trainData))
    println("auc2:"+auc2) //auc2:0.9545391695085871
    
    
    
    /*调整模型参数*/
    
    val evaluations = 
        for(rank <- Array(10,50);
            lambda <- Array(1.0,0.0001);
            alpha <- Array(1.0, 40.0))
        yield{
            val model = ALS.trainImplicit(trainData2, rank, 10, lambda, alpha)
            val auc = areaUnderCurve(cvData, bAllItemIDs, model.predict)
            ((rank,lambda,alpha),auc)
        }
    
    evaluations.sortBy(_._2).reverse.foreach(println)
    
  }//main
  
  
    def buildRatings(
      rawUserArtistData: RDD[String],
      bArtistAlias: Broadcast[scala.collection.Map[Int,Int]]) = {
        rawUserArtistData.map { line =>
          val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
          val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
          Rating(userID, finalArtistID, count)
        }
    }  
 
    def areaUnderCurve(
      positiveData: RDD[Rating],
      bAllItemIDs: Broadcast[Array[Int]],
      predictFunction: (RDD[(Int,Int)] => RDD[Rating])) = {
        // What this actually computes is AUC, per user. The result is actually something
        // that might be called "mean AUC".

        // Take held-out data as the "positive", and map to tuples
        val positiveUserProducts = positiveData.map(r => (r.user, r.product))
        // Make predictions for each of them, including a numeric score, and gather by user
        val positivePredictions = predictFunction(positiveUserProducts).groupBy(_.user)
        
        // BinaryClassificationMetrics.areaUnderROC is not used here since there are really lots of
        // small AUC problems, and it would be inefficient, when a direct computation is available.

        // Create a set of "negative" products for each user. These are randomly chosen
        // from among all of the other items, excluding those that are "positive" for the user.
        val negativeUserProducts = positiveUserProducts.groupByKey().mapPartitions {
            // mapPartitions operates on many (user,positive-items) pairs at once
            userIDAndPosItemIDs => {
               // Init an RNG and the item IDs set once for partition
                val random = new Random()
                val allItemIDs = bAllItemIDs.value
                userIDAndPosItemIDs.map { case (userID, posItemIDs) =>
                    val posItemIDSet = posItemIDs.toSet
                    val negative = new ArrayBuffer[Int]()
                    var i = 0
                    // Keep about as many negative examples per user as positive.
                    // Duplicates are OK
                    while (i < allItemIDs.size && negative.size < posItemIDSet.size) {
                        val itemID = allItemIDs(random.nextInt(allItemIDs.size))
                        if (!posItemIDSet.contains(itemID)) {
                            negative += itemID
                        }
                        i += 1
                    }
                    // Result is a collection of (user,negative-item) tuples
                    negative.map(itemID => (userID, itemID))
                }
           }
        }.flatMap(t => t)
        // flatMap breaks the collections above down into one big set of tuples

        // Make predictions on the rest:
        val negativePredictions = predictFunction(negativeUserProducts).groupBy(_.user)

        // Join positive and negative by user
        positivePredictions.join(negativePredictions).values.map {
             case (positiveRatings, negativeRatings) =>
            // AUC may be viewed as the probability that a random positive item scores
            // higher than a random negative one. Here the proportion of all positive-negative
            // pairs that are correctly ranked is computed. The result is equal to the AUC metric.
            var correct = 0L
            var total = 0L
            // For each pairing,
            for (positive <- positiveRatings;
                 negative <- negativeRatings) {
                  // Count the correctly-ranked pairs
              if (positive.rating > negative.rating) {
                correct += 1
              }
              total += 1
            }
            // Return AUC: fraction of pairs ranked correctly
            correct.toDouble / total
        }.mean() // Return mean AUC over users
        
     }//def areaUnderCurve

    
    /*简单暴力推荐：向每个用户推荐播放最多的艺术家*/
    def predictMostListened(
        sc:SparkContext,
        train:RDD[Rating])(allData:RDD[(Int, Int)])= {
            
        val bListenCount = sc.broadcast(
            train.map(rdd=>(rdd.product, rdd.rating)).reduceByKey(_+_).collectAsMap()        
        )
        allData.map{ case(user,product)=>
            Rating(user,product,bListenCount.value.getOrElse(product, 0.0))
            
        }
                
            
    }
    
    
    
}