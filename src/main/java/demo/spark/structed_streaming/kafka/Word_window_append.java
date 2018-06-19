package demo.spark.structed_streaming.kafka;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

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


 窗口聚合 append

 **使用数据中的时间戳**


 root
 |-- word: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)

 root
 |-- window: struct (nullable = true)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 |-- word: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)

 -------------------------------------------
 Batch: 0
 -------------------------------------------
 +------+----+---------+
 |window|word|timestamp|
 +------+----+---------+
 +------+----+---------+

 -------------------------------------------
 Batch: 1
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:00, 2018-04-03 16:06:10]|c   |2018-04-03 16:06:06|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|c   |2018-04-03 16:06:06|
 |[2018-04-03 16:06:00, 2018-04-03 16:06:10]|d   |2018-04-03 16:06:07|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|d   |2018-04-03 16:06:07|
 |[2018-04-03 16:06:00, 2018-04-03 16:06:10]|e   |2018-04-03 16:06:08|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|e   |2018-04-03 16:06:08|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|e   |2018-04-03 16:06:08|
 |[2018-04-03 16:06:00, 2018-04-03 16:06:10]|f   |2018-04-03 16:06:09|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|f   |2018-04-03 16:06:09|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|f   |2018-04-03 16:06:09|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|a   |2018-04-03 16:06:10|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|a   |2018-04-03 16:06:10|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|b   |2018-04-03 16:06:11|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|b   |2018-04-03 16:06:11|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|c   |2018-04-03 16:06:12|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|c   |2018-04-03 16:06:12|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|c   |2018-04-03 16:06:12|
 |[2018-04-03 16:06:04, 2018-04-03 16:06:14]|d   |2018-04-03 16:06:13|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|d   |2018-04-03 16:06:13|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|d   |2018-04-03 16:06:13|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 2
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|e   |2018-04-03 16:06:14|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|e   |2018-04-03 16:06:14|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|f   |2018-04-03 16:06:15|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|f   |2018-04-03 16:06:15|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 3
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|a   |2018-04-03 16:06:16|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|a   |2018-04-03 16:06:16|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|a   |2018-04-03 16:06:16|
 |[2018-04-03 16:06:08, 2018-04-03 16:06:18]|b   |2018-04-03 16:06:17|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|b   |2018-04-03 16:06:17|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|b   |2018-04-03 16:06:17|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|c   |2018-04-03 16:06:18|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|c   |2018-04-03 16:06:18|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|d   |2018-04-03 16:06:19|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|d   |2018-04-03 16:06:19|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 4
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|e   |2018-04-03 16:06:20|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|e   |2018-04-03 16:06:20|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|e   |2018-04-03 16:06:20|
 |[2018-04-03 16:06:12, 2018-04-03 16:06:22]|f   |2018-04-03 16:06:21|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|f   |2018-04-03 16:06:21|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|f   |2018-04-03 16:06:21|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|a   |2018-04-03 16:06:22|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|a   |2018-04-03 16:06:22|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|b   |2018-04-03 16:06:23|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|b   |2018-04-03 16:06:23|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 5
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|c   |2018-04-03 16:06:24|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|c   |2018-04-03 16:06:24|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|c   |2018-04-03 16:06:24|
 |[2018-04-03 16:06:16, 2018-04-03 16:06:26]|d   |2018-04-03 16:06:25|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|d   |2018-04-03 16:06:25|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|d   |2018-04-03 16:06:25|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|e   |2018-04-03 16:06:26|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|e   |2018-04-03 16:06:26|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|f   |2018-04-03 16:06:27|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|f   |2018-04-03 16:06:27|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 6
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|a   |2018-04-03 16:06:28|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|a   |2018-04-03 16:06:28|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|a   |2018-04-03 16:06:28|
 |[2018-04-03 16:06:20, 2018-04-03 16:06:30]|b   |2018-04-03 16:06:29|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|b   |2018-04-03 16:06:29|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|b   |2018-04-03 16:06:29|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|c   |2018-04-03 16:06:30|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|c   |2018-04-03 16:06:30|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|d   |2018-04-03 16:06:31|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|d   |2018-04-03 16:06:31|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 7
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|e   |2018-04-03 16:06:32|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|e   |2018-04-03 16:06:32|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|e   |2018-04-03 16:06:32|
 |[2018-04-03 16:06:24, 2018-04-03 16:06:34]|f   |2018-04-03 16:06:33|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|f   |2018-04-03 16:06:33|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|f   |2018-04-03 16:06:33|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|a   |2018-04-03 16:06:34|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|a   |2018-04-03 16:06:34|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|b   |2018-04-03 16:06:35|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|b   |2018-04-03 16:06:35|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 8
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|c   |2018-04-03 16:06:36|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|c   |2018-04-03 16:06:36|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|c   |2018-04-03 16:06:36|
 |[2018-04-03 16:06:28, 2018-04-03 16:06:38]|d   |2018-04-03 16:06:37|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|d   |2018-04-03 16:06:37|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|d   |2018-04-03 16:06:37|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|e   |2018-04-03 16:06:38|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|e   |2018-04-03 16:06:38|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|f   |2018-04-03 16:06:39|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|f   |2018-04-03 16:06:39|
 +------------------------------------------+----+-------------------+

 -------------------------------------------
 Batch: 9
 -------------------------------------------
 +------------------------------------------+----+-------------------+
 |window                                    |word|timestamp          |
 +------------------------------------------+----+-------------------+
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|a   |2018-04-03 16:06:40|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|a   |2018-04-03 16:06:40|
 |[2018-04-03 16:06:40, 2018-04-03 16:06:50]|a   |2018-04-03 16:06:40|
 |[2018-04-03 16:06:32, 2018-04-03 16:06:42]|b   |2018-04-03 16:06:41|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|b   |2018-04-03 16:06:41|
 |[2018-04-03 16:06:40, 2018-04-03 16:06:50]|b   |2018-04-03 16:06:41|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|c   |2018-04-03 16:06:42|
 |[2018-04-03 16:06:40, 2018-04-03 16:06:50]|c   |2018-04-03 16:06:42|
 |[2018-04-03 16:06:36, 2018-04-03 16:06:46]|d   |2018-04-03 16:06:43|
 |[2018-04-03 16:06:40, 2018-04-03 16:06:50]|d   |2018-04-03 16:06:43|
 +------------------------------------------+----+-------------------+







 */
public class Word_window_append {
    private static final String bootstrapServers = "SparkMaster:9092";
    private static final String topics = "flinktest";  //topic1,topic2
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("").master("local[5]")
                .config("spark.sql.shuffle.partitions", 5) //https://stackoverflow.com/questions/45704156/what-is-the-difference-between-spark-sql-shuffle-partitions-and-spark-default-pa
                .config("spark.default.parallelism", 5)
                .getOrCreate();
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "flinktest")
                //.option("startingOffsets","latest")
                .load()
                .selectExpr("CAST(value AS STRING)");


        Dataset<Row> words = lines.map(new MapFunction<Row, Tuple2<String, Timestamp>>() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            @Override
            public Tuple2<String, Timestamp> call(Row row) throws Exception {
                String[] characterAndTime = row.mkString().split(":");
                String time = simpleDateFormat.format(new Date(Long.valueOf(characterAndTime[1])));
                return new Tuple2<>(characterAndTime[0], Timestamp.valueOf(time));
            }
        }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF("word", "timestamp");

        words.printSchema();
//        root
//         |-- word: string (nullable = true)
//         |-- timestamp: timestamp (nullable = true)

        //这个需要提到前面，否者如果使用words.withWatermark("timestamp", "5 seconds").groupBy( functions.window(words.col("timestamp"), "10 seconds", "4 seconds")
        //                                                 ).count() 就会报错了。因为groupBy里window使用的是words.col("")还未watermark。正确应该使用watermark之后的rdd：wordsWithWatermark
        Dataset<Row> wordsWithWatermark = words.withWatermark("timestamp", "5 seconds");

        // 注意这里
//        RelationalGroupedDataset timestamp = wordsWithWatermark.groupBy(functions.window(wordsWithWatermark.col("timestamp"), "10 seconds", "4 seconds"));
        Dataset<Row> wordsWithWindow = wordsWithWatermark.select(functions.window(wordsWithWatermark.col("timestamp"), "10 seconds", "4 seconds").alias("window"), wordsWithWatermark.col("word"), wordsWithWatermark.col("timestamp"));


        wordsWithWindow.printSchema();
        wordsWithWindow.createOrReplaceTempView("words");


        StreamingQuery query = spark.sql("select * from words").writeStream().trigger(Trigger.ProcessingTime(4000)).outputMode("append").format("console").option("truncate", "false").start();

        query.awaitTermination();

    }
}
