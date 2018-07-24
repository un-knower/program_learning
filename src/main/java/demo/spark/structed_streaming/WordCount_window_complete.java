package demo.spark.structed_streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import scala.Tuple2;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
API是通过一个全局的window方法来设置，如下所示是其Spark实现细节：

 def window(timeColumn:Column, windowDuratiion:String, slideDuration:String):Column ={

 window(timeColumn, windowDuration, slideDuration, "0" second)

 }

 timecolumn:具有时间戳的列；
 windowDuration：为窗口的时间长度；
 slideDuration：为滑动的步长；
 return：返回的数据类型是Column。

 窗口时间：是指确定数据操作的长度；
 滑动步长：是指窗口每次向前移动的时间长度；
 触发时间：是指Structured Streaming将数据写入外部DataStreamWriter的时间间隔。


 窗口聚合 complete



输入
 root@sparkmaster:/usr/local# nc -lk 9999
 1 2 3 4
 1 2 3 4

输出
 -------------------------------------------
 Batch: 0
 -------------------------------------------
 +------------------------------------------+----+-----+
 |window                                    |word|count|
 +------------------------------------------+----+-----+
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|2   |1    |
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|1   |1    |
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|4   |1    |
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|3   |1    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|1   |1    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|4   |1    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|3   |1    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|2   |1    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|1   |1    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|3   |1    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|4   |1    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|2   |1    |
 +------------------------------------------+----+-----+

 -------------------------------------------
 Batch: 1
 -------------------------------------------
 +------------------------------------------+----+-----+
 |window                                    |word|count|
 +------------------------------------------+----+-----+
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|2   |2    |
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|1   |2    |
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|4   |2    |
 |[1970-01-18 22:54:44, 1970-01-18 22:54:49]|3   |2    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|1   |2    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|4   |2    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|3   |2    |
 |[1970-01-18 22:54:46, 1970-01-18 22:54:51]|2   |2    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|1   |2    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|3   |2    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|4   |2    |
 |[1970-01-18 22:54:48, 1970-01-18 22:54:53]|2   |2    |
 +------------------------------------------+----+-----+
 */
public class WordCount_window_complete {
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("").master("local[2]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> lines = spark.readStream()
                                   .format("socket")
                                   .option("host", "sparkmaster")
                                   .option("port", 9999)
                                   //.option("includeTimestamp", true) //输出内容包括时间戳
                                   .load();


        Dataset<Row> words = lines.withColumn("timestamp", functions.current_timestamp()).as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
                    List<Tuple2<String, Timestamp>> result = new ArrayList<>();
                    for (String word : t._1.split(" ")) {
                        result.add(new Tuple2<>(word, t._2));
                    }
                    return result.iterator();
                },
                Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
        ).toDF("word", "timestamp");
        Dataset<Row> wordsWithtime = words.withWatermark("timestamp", "10 seconds");
        Dataset<Row> windowedCounts = wordsWithtime.groupBy(
                                                 functions.window(words.col("timestamp"),
                                                 "10 seconds",
                                                 "5 seconds"),
                                                 words.col("word"))
                                           .count()
                                           .orderBy("window");

        //由于采用聚合操作，所以需要指定"complete"输出形式。指定"truncate"只是为了在控制台输出时，不进行列宽度自动缩小。
        StreamingQuery query = windowedCounts.writeStream().outputMode("complete").format("console").option("truncate", "false").start();
        query.awaitTermination();

    }


}
