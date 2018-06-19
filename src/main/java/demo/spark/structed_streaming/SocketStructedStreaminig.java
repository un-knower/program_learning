package demo.spark.structed_streaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

/**
 * @author bjwangguangliang
 */
public class SocketStructedStreaminig {
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("JavaStructuredNetworkWordCount").master("local[2]").getOrCreate();

        Dataset<Row> lines = spark.readStream().format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        Dataset<String> words = lines
                                    .as(Encoders.STRING())
                                    .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console

        StreamingQuery query = wordCounts.writeStream()//.trigger(Trigger.ProcessingTime(3000, TimeUnit.MILLISECONDS))
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
