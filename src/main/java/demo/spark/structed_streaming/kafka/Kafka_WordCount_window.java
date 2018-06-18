package demo.spark.structed_streaming.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class Kafka_WordCount_window {
    private static final String bootstrapServers = "SparkMaster:9092";
    private static final String topics = "flinktest";  //topic1,topic2
    public static void main(String args[]) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.ERROR);

        StructType structType = new StructType().add("value","string").add("timestamp","timestamp");

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructuredKafkaWordCount").master("local[2]")
                .getOrCreate();

        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "flinktest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

//        Dataset<Row> words = lines.withColumn("timestamp", functions.current_timestamp()).as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).flatMap(
//                (FlatMapFunction<Tuple2<String, Timestamp>, Row>) t -> {
//                    List<Row> result = new ArrayList<>();
//                    Object[] object = new Object[2];
//                    for (String word : t._1.split("")) {
//                        object[0] = word;
//                        object[1] = t._2;
//                        Row newRow = new GenericRowWithSchema(object, structType);
//                        result.add(newRow);
//                    }
//                    return result.iterator();
//                }, RowEncoder.apply(structType));




        // window内的总字符个数count
//        Dataset<Row> windowedCounts = lines.selectExpr("CAST(value as STRING)","CAST(timestamp as timestamp)").as(RowEncoder.apply(structType)).withWatermark("timestamp", "10 seconds").groupBy(
//                functions.window(lines.col("timestamp"),
//                        "10 seconds",
//                        "4 seconds"))
//
//                .count();
        Dataset<Row> linesWithWaterMark = lines.selectExpr("CAST(value as STRING)", "CAST(timestamp as timestamp)").as(RowEncoder.apply(structType)).withWatermark("timestamp", "10 seconds");
        Dataset<Row> windowedCounts = linesWithWaterMark.groupBy(functions.window(linesWithWaterMark.col("timestamp"), "10 seconds", "4 seconds")).count();


        // Start running the query that prints the running counts to the console
        StreamingQuery query = windowedCounts.writeStream()
                .outputMode("append").option("truncate", "false")
                .format("console")
                .start();

        query.awaitTermination();


    }
}
