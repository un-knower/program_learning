package demo.spark.structed_streaming.kafka;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class Kafka_WordCount {
    private static final String bootstrapServers = "SparkMaster:9092";
    private static final String topics = "flinktest";  //topic1,topic2
    public static void main(String args[]) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.ERROR);

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
                //.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") //Try to map struct<key:string,value:string> to Tuple1, but failed as the number of fields does not line up.;
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = lines.flatMap(
                (FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING()).groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();


    }
}
