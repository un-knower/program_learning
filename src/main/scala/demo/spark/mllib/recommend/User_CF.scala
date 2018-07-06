package demo.spark.mllib.recommend

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.collection.immutable.Range.Int
import scala.util.{Success, Try}
object User_CF {
  def verify(str: String, dtype: String):Boolean = {

    var c:Try[Any] = null
    if("double".equals(dtype)) {
      c = scala.util.Try(str.toDouble)
    } else if("int".equals(dtype)) {
      c = scala.util.Try(str.toInt)
    }
    val result = c match {
      case Success(_) => true;
      case _ =>  false;
    }
    result
  }

  def uid_topN(itemSimi: ItemSimi)= {
    val uid1 = itemSimi.itemid1
    val uid2 = itemSimi.itemid2
    val simi = itemSimi.similar
  }

  def main(args: Array[String]) {

    //0 构建Spark对象
    val conf = new SparkConf().setAppName("User_CF")//.setMaster("local[*]")
    conf.set("spark.shuffle.io.retryWait", "10s")
    conf.set("spark.rdd.compress", "true")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.locality.wait","1s")
    conf.set("spark.memory.storageFraction","0.5")
    conf.set("spark.speculation", "true")

    val WINDOW_WIDTH = 172800  // 2 day 前时间
    val PARTITION_NUM = 380 //分区数
    val RANDOM_KEY = 1200 //
    val VIDEO_PREFIX = "9IG74V5H00963VRO_video_"
    val TOPN = 4000
    val HEADLINE_PREFIX = "headline_ctr_"
    val NOW_TS = Integer.parseInt(args(0).trim)  //当前时间
    val LAST_TS = NOW_TS - 172800

    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    val cur_ts = Integer.parseInt(args(0).trim)
    val path_num = Integer.parseInt(args(1).trim)
    val path =  new ArrayBuffer[String]()

    for(idx <- 0 until path_num) {
       path += args(idx+2).trim
    }

    //1 读取样本数据
  
    var data = sc.textFile(path(0), PARTITION_NUM)
    for(idx <- 1 until path.length) {
      val tmp_path = sc.textFile(path(idx))
      data = data.union(tmp_path)
    }

    val userdata = data.repartition(PARTITION_NUM).map(_.replaceAll("\\(|\\)|\\'", "")).map{x=>
      val arr = x.split(",")
      (arr(0).trim,arr(1).trim,Integer.parseInt(arr(2).trim))
      }.repartition(PARTITION_NUM)
      .filter{f =>
        val docid = f._1
        val click_time = f._3
        var isFilter = false
        if(click_time > LAST_TS) {
           if(docid.length==16 && verify(docid.substring(8,12),"int")) {
              isFilter=true
           }
           if(docid.startsWith("V")) {
              isFilter=true
           }
         }
         isFilter
        }

    .map(f => (ItemPref(f._1, f._2, 1.0))).repartition(PARTITION_NUM)



    //2 建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1 = mysimil.Similarity(userdata, "cooccurrence")
    val result = simil_rdd1.map(itemSimi=>(itemSimi.itemid1,(itemSimi.itemid2,itemSimi.similar))).groupByKey.map(f => {
      val uid = f._1
      val i2 = f._2.toBuffer
      val i2_2 = i2.sortBy(_._2)
      val res = new ArrayBuffer[String]()
      if (i2_2.length > TOPN) i2_2.remove(0, (i2_2.length - TOPN))

      for(idx <- Range(i2_2.length,0,-1)) {
        val tmp = i2_2(idx-1)
        res += tmp._1+"\2"+tmp._2
      }


      uid+"\1"+res.mkString("\2")
    })

    result.saveAsTextFile("/user/datacenter/wguangliang/recommend/user_base_test/doc-item-base-"+NOW_TS)
    
    //result.collect().foreach(println)
    
    sc.stop()

//    val recommd = new RecommendedItem
//    val recommd_rdd1 = recommd.Recommend(simil_rdd1, userdata, TOPN)
//
//    //3 打印结果
//    println(s"物品相似度矩阵: ${simil_rdd1.count()}")
//    simil_rdd1.collect().foreach { ItemSimi =>
//      println(ItemSimi.itemid1 + ", " + ItemSimi.itemid2 + ", " + ItemSimi.similar)
//    }
//    println("-"*10)
//
//    println(s"用戶推荐列表: ${recommd_rdd1.count()}")
//    recommd_rdd1.collect().foreach { UserRecomm =>
//      println(UserRecomm.userid + ", " + UserRecomm.itemid + ", " + UserRecomm.pref)
//    }





  }
}

