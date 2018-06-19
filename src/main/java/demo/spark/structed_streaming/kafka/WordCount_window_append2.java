package demo.spark.structed_streaming.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

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

 使用程序接收时间戳

 */
public class WordCount_window_append2 {
    private static final String bootstrapServers = "SparkMaster:9092";
    private static final String topics = "flinktest";  //topic1,topic2
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("").master("local[3]")
                .config("spark.sql.shuffle.partitions",3)
                .config("spark.default.parallelism",3)
                .getOrCreate();
        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "flinktest")
                //.option("startingOffsets","latest")
                .load()
                .selectExpr("CAST(value AS STRING)", "CAST(timestamp as timestamp)");




        lines.printSchema();
//        root
//         |-- word: string (nullable = true)
//         |-- timestamp: timestamp (nullable = true)

        Dataset<Row> wordsWithWatermark = lines.withWatermark("timestamp", "5 seconds");

        //这个需要提到前面，否者如果使用words.withWatermark("timestamp", "5 seconds").groupBy( functions.window(words.col("timestamp"), "10 seconds", "4 seconds")
        //                                                 ).count() 就会报错了。因为groupBy里window使用的是words.col("")还未watermark。正确应该使用watermark之后的rdd：wordsWithWatermark
//        Dataset<Row> wordsWithWatermark = words.withWatermark("timestamp", "5 seconds");
//        // 注意这里
        Dataset<Row> windowedCounts = wordsWithWatermark.groupBy( functions.window(wordsWithWatermark.col("timestamp"), "10 seconds", "4 seconds")
                                                 ).count();



        //由于采用聚合操作，所以需要指定"complete"输出形式。指定"truncate"只是为了在控制台输出时，不进行列宽度自动缩小。
        StreamingQuery query = windowedCounts.writeStream().trigger(Trigger.ProcessingTime(4000)).outputMode("append").format("console").option("truncate", "false").start();
        query.awaitTermination();

    }


}
