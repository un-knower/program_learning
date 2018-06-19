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
在Structured Streaming 里，多出了outputMode,现在有complete,append,update 三种

complete,每次计算完成后，你都能拿到全量的计算结果。
append,每次计算完成后，你能拿到增量的计算结果。
update，只显示更新的值，也是所全局的累计
但是，这里有个但是，使用了聚合类函数才能用complete模式，只是简单的使用了map,filter等才能使用append模式。

 */
public class WordCount_append {
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("JavaStructuredNetworkWordCount").master("local[2]").getOrCreate();

        Dataset<Row> lines = spark.readStream().format("socket")
                .option("host", "sparkmaster")
                .option("port", 9999)
                .load();

        Dataset<String> words = lines
                                    .as(Encoders.STRING())
                                    .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());



        // Start running the query that prints the running counts to the console

        StreamingQuery query = words.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
