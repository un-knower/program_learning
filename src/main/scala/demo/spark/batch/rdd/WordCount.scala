package demo.spark.batch.rdd

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
/**
 * 使用wordcount测试本地Spark环境
 */
class WordCount {
  
}
object WordCount {
    def main(args:Array[String]) {
        /*
         * 第一步，创建Spark的配置对象SparkConf
         * 设置Spark程序的运行时的配置信息
         * 例如，通过setMaster来设置程序要连接的Spark集群的Master的URL
         * 如果设置为local，则代表Spark程序在本地运行
         */
        val conf = new SparkConf  //创建SparkConf对象
        conf.setAppName("WordCount Spark App") //设置应用程序的名称，在程序运行的监控界面中可以看到名称
//        conf.setMaster("local[2]") //local[2]程序在本地运行，2个线程，不需要安装Spark集群
        conf.setMaster("spark://SparkMaster:7077") //Spark集群运行，需要从hdfs中读取文件。在集群下环境运行要打成jar包，注释掉，使用submit传参
        //conf.setMaster("local[4]");
        /**
         * 打jar包 成  ：  JAR file
         
         #/bin/bash
          /usr/local/spark/spark-1.5.1/bin/spark-submit
           --class com.spark.demo.WordCount \
           --master spark://SparkMaster:7077 \
           /data/spark/wordcount.jar
          
                          运行:
            bash wordcount.sh
          
         */
        /**
         * 第二步，创建SparkContext对象
         * SparkContext是Spark程序所有功能的唯一入口
         * 无论采用Scala,Java,Python,R等都必须有一个SparkContext
         * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGSchedule、TaskScheduler、SchedulerBackend
         * 同时还会负责Spark程序往Master注册程序
         * SparkContext是整个Spark应用程序中最为至关重要的一个对象
         */
        val sc = new SparkContext(conf) //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行时环境
        
        /**
         * 第三步，根据具体的数据来源(HDFS、HBase、Local FS、DB、S3等)通过SparkContext来创建RDD
         * RDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala集合、由其他的RDD操作转换
         * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的的数据属于一个Task的处理范畴;一个Partition对应一个Task
         */
 //       val lines = sc.textFile("file/wordcount/LICENSE", 1) //读取本地文件，并设置Partition数量
       val lines = sc.textFile("hdfs://sparkmaster:9000/eclipse/multiwordcount/multiwordcount", 1) //集群运行，从hdfs中读取
//        val lines:RDD[String] = sc.textFile("file/wordcount/LICENSE", 1) //读取本地文件，并设置Partition数量
        
        /**
         * 第四步：对初始的RDD进行Transformation级别的处理。例如map,filter等高阶函数等的编程，来进行具体的数据计算
         * 第4.1 对每一行的字符串进行单词切分并把所有行拆分结果通过flat合并成为一个集合
         * 第4.2 在单词拆分的基础上对每个单词实例计数为1，
         * 第4.3 在对每个单词实例计数为1的基础上统计每个单词在文件中出现的总次数，
         */
        val wordPair = lines.flatMap { line => line.split(" ") }.map { word => (word,1) }
        val wordCount = wordPair.reduceByKey(_+_) //对相同的key，进行value的累计
        //wordCount.foreach(wordNumberPair => println(wordNumberPair._1 +":"+ wordNumberPair._2)) 这个在spark集群中不会有结果打印出来，因为没有collect到driver端
        wordCount.collect.foreach(wordNumberPair => println(wordNumberPair._1 +":"+ wordNumberPair._2))
        
        Thread.sleep(10 * 60 * 1000) // 挂住 10 分钟; 这时可以去看 SparkUI: http://localhost:4040
        sc.stop()
                
    }
}