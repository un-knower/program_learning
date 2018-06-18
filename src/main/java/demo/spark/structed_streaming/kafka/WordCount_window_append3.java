package demo.spark.structed_streaming.kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

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
public class WordCount_window_append3 {
    private static final String bootstrapServers = "SparkMaster:9092";
    private static final String topics = "flinktest";  //topic1,topic2
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("test").master("local[3]")
                .config("spark.sql.shuffle.partitions",3)
                .config("spark.default.parallelism",3)
                .getOrCreate();

        Dataset<Row> rowDataset = spark.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "sparkmaster:9092")
                .option("subscribe", "flinktest")
                .load();

        StructType type = new StructType().add("value","string").add("timestamp","timestamp");

        Dataset<Row> row =
                rowDataset.selectExpr("CAST(value as STRING)","CAST(timestamp as timestamp)")
                        .as(RowEncoder.apply(type))
                        .withWatermark("timestamp","5 seconds");
//                .withColumn("curTime",current_timestamp());
//        row.explain(true);
        row.printSchema();
        StreamingQuery query=       row

                .groupBy(functions.window(row.col("timestamp"),"10 seconds","4 seconds"))
                .count()
                .writeStream()
                .trigger(Trigger.ProcessingTime(4000)).outputMode("append")
                .format("console").option("truncate",false)
                .start();
//
        query.awaitTermination();

    }


}
