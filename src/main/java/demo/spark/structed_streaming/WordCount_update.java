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



root@sparkmaster:/home/gralion# nc -l 9999
1 2 3 4
2 3 4 5
1 2 3 4

-------------------------------------------
Batch: 0
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|    3|    1|
|    1|    1|
|    4|    1|
|    2|    1|
+-----+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|    3|    2|
|    5|    1|
|    4|    2|
|    2|    2|
+-----+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----+-----+
|value|count|
+-----+-----+
|    3|    3|
|    1|    2|
|    4|    3|
|    2|    3|
+-----+-----+
 */
public class WordCount_update {
    public static void main(String args[]) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("JavaStructuredNetworkWordCount").master("local[2]").getOrCreate();

        Dataset<Row> lines = spark.readStream().format("socket")
                .option("host", "sparkmaster")
                .option("port", 9999)
                .load();

        Dataset<String> words = lines
                                    .as(Encoders.STRING())
                                    .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("update")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
