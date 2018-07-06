package demo.spark.structed_streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

/**
 * Author: wguangliang
 * Date: 2018/4/2
 * Description:
 */
public class TestClass {

    @Test
    public void testWindow() throws Exception{
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
