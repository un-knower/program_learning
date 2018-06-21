package demo.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vectors,Vector}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD

/**
 * 网络入侵异常检测
 * kmeans
 */
object KddCupKMeans {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Kmean").setMaster("local")
        val sc = new SparkContext(conf)
        val rawData = sc.textFile("C:\\D\\数据\\KDD Cup 1999 Data\\kddcup.data.corrected")
        rawData.take(5).foreach(println)
        
//0,tcp,http,SF,215,45076,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,normal.
//0,tcp,http,SF,162,4528,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,2,2,0.00,0.00,0.00,0.00,1.00,0.00,0.00,1,1,1.00,0.00,1.00,0.00,0.00,0.00,0.00,0.00,normal.
//0,tcp,http,SF,236,1228,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,1,1,0.00,0.00,0.00,0.00,1.00,0.00,0.00,2,2,1.00,0.00,0.50,0.00,0.00,0.00,0.00,0.00,normal.
//0,tcp,http,SF,233,2032,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,2,2,0.00,0.00,0.00,0.00,1.00,0.00,0.00,3,3,1.00,0.00,0.33,0.00,0.00,0.00,0.00,0.00,normal.
//0,tcp,http,SF,239,486,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,3,3,0.00,0.00,0.00,0.00,1.00,0.00,0.00,4,4,1.00,0.00,0.25,0.00,0.00,0.00,0.00,0.00,normal.

        rawData.map(_.split(",").last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
//(smurf.,2807886)
//(neptune.,1072017)
//(normal.,972781)
//(satan.,15892)
//(ipsweep.,12481)
//(portsweep.,10413)
//(nmap.,2316)
//(back.,2203)
//(warezclient.,1020)
//(teardrop.,979)
//(pod.,264)
//(guess_passwd.,53)
//(buffer_overflow.,30)
//(land.,21)
//(warezmaster.,20)
//(imap.,12)
//(rootkit.,10)
//(loadmodule.,9)
//(ftp_write.,8)
//(multihop.,7)
//(phf.,4)
//(perl.,3)
//(spy.,2)        
         //2,3,4列是字符，不是数值类型的，最后标号也不是数值类型的。而kmeans要求特征为数值类型
        val labelsAndData = rawData.map { line => 
            val buffer = line.split(",").toBuffer
            buffer.remove(1, 3)
            val label = buffer.remove(buffer.length-1)
            val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
            (label,vector)
        }
        
        val data = labelsAndData.values.cache()
        val kmeans = new KMeans
        val model = kmeans.run(data)
        model.clusterCenters.foreach(println)
//可以看出，程序分出了两类
//[48.34019491959669,1834.6215497618625,826.2031900016945,5.7161172049003456E-6,6.487793027561892E-4,7.961734678254053E-6,0.012437658596734055,3.205108575604837E-5,0.14352904910348827,0.00808830584493399,6.818511237273984E-5,3.6746467745787934E-5,0.012934960793560386,0.0011887482315762398,7.430952366370449E-5,0.0010211435092468404,0.0,4.082940860643104E-7,8.351655530445469E-4,334.9735084506668,295.26714620807076,0.17797031701994304,0.17803698940272675,0.05766489875327384,0.05772990937912762,0.7898841322627527,0.021179610609915762,0.02826081009629794,232.98107822302248,189.21428335201279,0.753713389800417,0.030710978823818437,0.6050519309247937,0.006464107887632785,0.1780911843182427,0.17788589813471198,0.05792761150001037,0.05765922142400437]
//[10999.0,0.0,1.309937401E9,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,0.0,1.0,1.0,1.0,0.0,0.0,255.0,1.0,0.0,0.65,1.0,0.0,0.0,0.0,1.0,1.0]
        
        val clusterLabelCount = labelsAndData.map{case(label, datum)=>
            val cluster = model.predict(datum)
            (cluster,label)
        }.countByValue()
        
        clusterLabelCount.toSeq.sorted.foreach{
            case((cluster,label),count)=>
                println(f"$cluster%1s$label%18s$count%8s")
        }
//0             back.    2203
//0  buffer_overflow.      30
//0        ftp_write.       8
//0     guess_passwd.      53
//0             imap.      12
//0          ipsweep.   12481
//0             land.      21
//0       loadmodule.       9
//0         multihop.       7
//0          neptune. 1072017
//0             nmap.    2316
//0           normal.  972781
//0             perl.       3
//0              phf.       4
//0              pod.     264
//0        portsweep.   10412
//0          rootkit.      10
//0            satan.   15892
//0            smurf. 2807886
//0              spy.       2
//0         teardrop.     979
//0      warezclient.    1020
//0      warezmaster.      20
//1        portsweep.       1      
        
        
        //结果显示聚类根本没有任何作用，簇1只有1个数据点
        
        //所以要定义k的选择，好的聚类结果是每个数据点都紧靠最近的质心
        //因此定义欧式距离表示距离
        def distance(a:Vector, b:Vector)={
            val pair = a.toArray.zip(b.toArray)
            math.sqrt(pair.map(p=>p._1 - p._2).map(d=>d*d).sum) 
        }
        //数据到该类质心的距离
        def distToCentroid(datum:Vector,model:KMeansModel) = {
            val cluster = model.predict(datum)
            val centoid = model.clusterCenters(cluster)
            distance(centoid, datum)
        }
        //为一个给定k值的模型定义平均质心距离函数
        def clusterScore(data:RDD[Vector], k:Int)= {
            val kmeans = new KMeans
            kmeans.setK(k)
            val model = kmeans.run(data)
            data.map { datum => distToCentroid(datum, model) }.mean()
        }
        
        (5 to 40 by 5).map(k =>
            (k, clusterScore(data, k))    
        ).foreach(println)
        
        
    }
  
}