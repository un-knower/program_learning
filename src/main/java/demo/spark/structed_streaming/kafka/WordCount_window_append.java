package demo.spark.structed_streaming.kafka;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple2;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

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

 -------------------------------------------
 Batch: 0
 -------------------------------------------
 +------+-----+
 |window|count|
 +------+-----+
 +------+-----+

 -------------------------------------------
 Batch: 1
 -------------------------------------------
 +------+-----+
 |window|count|
 +------+-----+
 +------+-----+

 -------------------------------------------
 Batch: 2
 -------------------------------------------
 +------+-----+
 |window|count|
 +------+-----+
 +------+-----+

 -------------------------------------------
 Batch: 3
 -------------------------------------------
 +------------------------------------------+-----+
 |window                                    |count|
 +------------------------------------------+-----+
 |[2018-04-03 11:01:08, 2018-04-03 11:01:18]|3    |
 +------------------------------------------+-----+

 -------------------------------------------
 Batch: 4
 -------------------------------------------
 +------------------------------------------+-----+
 |window                                    |count|
 +------------------------------------------+-----+
 |[2018-04-03 11:01:12, 2018-04-03 11:01:22]|7    |
 +------------------------------------------+-----+

 -------------------------------------------
 Batch: 5
 -------------------------------------------
 +------------------------------------------+-----+
 |window                                    |count|
 +------------------------------------------+-----+
 |[2018-04-03 11:01:16, 2018-04-03 11:01:26]|10   |
 +------------------------------------------+-----+

 -------------------------------------------
 Batch: 6
 -------------------------------------------
 +------------------------------------------+-----+
 |window                                    |count|
 +------------------------------------------+-----+
 |[2018-04-03 11:01:20, 2018-04-03 11:01:30]|10   |
 +------------------------------------------+-----+

 -------------------------------------------
 Batch: 7
 -------------------------------------------
 +------------------------------------------+-----+
 |window                                    |count|
 +------------------------------------------+-----+
 |[2018-04-03 11:01:24, 2018-04-03 11:01:34]|10   |
 +------------------------------------------+-----+

 -------------------------------------------
 Batch: 8
 -------------------------------------------
 +------------------------------------------+-----+
 |window                                    |count|
 +------------------------------------------+-----+
 |[2018-04-03 11:01:28, 2018-04-03 11:01:38]|10   |
 +------------------------------------------+-----+

 */
public class WordCount_window_append {

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
        Dataset<Row> windowedCounts = wordsWithWatermark.groupBy( functions.window(wordsWithWatermark.col("timestamp"), "10 seconds", "4 seconds")
                                                 ).count();



        //由于采用聚合操作，所以需要指定"complete"输出形式。指定"truncate"只是为了在控制台输出时，不进行列宽度自动缩小。
        StreamingQuery query = windowedCounts.writeStream().trigger(Trigger.ProcessingTime(4000)).outputMode("append").format("console").option("truncate", "false").start();
        query.awaitTermination();

    }
}
