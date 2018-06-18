package demo.spark.structed_streaming;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import scala.Tuple4;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author bjwangguangliang
 */
public class OnelineLearningExpClickJoin3 {
    private static final String bootstrapServers = "SparkMaster:9092";
    private static final String topics = "flinktest";  //topic1,topic2
    public static void main(String args[]) throws InterruptedException, StreamingQueryException, AnalysisException {
        // thrift请求体
        // FeatureExtractServiceRequest request = new FeatureExtractServiceRequest();

        SparkSession spark = SparkSession.builder()
                .appName("")
                .master("local[3]")
                .config("spark.sql.shuffle.partitions", 3)
                .config("spark.default.parallelism", 3)
                .getOrCreate();

        Dataset<String> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", "flinktest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        Dataset<Row> touTiaoData = lines.repartition(5).filter(new FilterFunction<String>() {
            @Override
            public boolean call(String line) throws Exception {
                System.out.println("filter:"+line);
                return (StringUtils.contains(line, "2x1kfBk63z") || StringUtils.contains(line, "2S5Wcx")) &&
                        (StringUtils.contains(line, "EV") || StringUtils.contains(line, "RCC"));
            }
        }).flatMap(new FlatMapFunction<String, Tuple4<String, Timestamp, String, String>>() {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            List<Tuple4<String, Timestamp, String, String>> result = new ArrayList<>();

            @Override
            public Iterator<Tuple4<String, Timestamp, String, String>> call(String line) throws Exception {
                try {

                    result.clear();
                    JSONObject jsonObject = JSONObject.parseObject(line);
                    String devId = jsonObject.getString("devId");
                    Timestamp timestamp = jsonObject.getTimestamp("timestamp");
                    String event = jsonObject.getString("n");
                    if ("EV".equals(event)) {
                        String kv = jsonObject.getString("kv");
                        if (kv != null) {
                            String docWithComma = JSONObject.parseObject(kv).getString("ids");
                            String[] docList = StringUtils.splitByWholeSeparatorPreserveAllTokens(docWithComma, ",");
                            for (String docId : docList) {
                                result.add(new Tuple4<>(devId, timestamp, event, docId));
                            }
                        }

                    } else if ("RCC".equals(event)) {
                        String kv = jsonObject.getString("kv");
                        if (kv != null) {
                            String docId = JSONObject.parseObject(kv).getString("id");
                            result.add(new Tuple4<>(devId, timestamp, event, docId));
                        }
                    }

                    return result.iterator();
                } catch (Exception e) {
                    e.printStackTrace();
                    return result.iterator();
                }
            }
        }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP(), Encoders.STRING(), Encoders.STRING())).toDF("deviceid", "time", "event", "doc");

        touTiaoData.withWatermark("time", "10 seconds");

        touTiaoData.createOrReplaceTempView("online");

        Dataset<Row> evDataSet = spark.sql("select * from online where event='EV'").withWatermark("time", "4 seconds");
        evDataSet.createOrReplaceTempView("ev");
        evDataSet.writeStream().outputMode("append").option("truncate", false).format("console").trigger(Trigger.ProcessingTime(4000)).start();
        Dataset<Row> clickDataSet = spark.sql("select * from online where event='RCC'").withWatermark("time", "10 seconds");
        clickDataSet.createOrReplaceTempView("click");
        clickDataSet.writeStream().outputMode("append").option("truncate", false).format("console").trigger(Trigger.ProcessingTime(4000)).start();


        Dataset<Row> result = spark.sql("select *,if(click.doc is null,0,1) from ev left outer join click on ev.deviceid=click.deviceid and ev.doc = click.doc and click.time >= ev.time and click.time <= ev.time + INTERVAL 10 seconds ");
        result.createOrReplaceTempView("result");


        result.printSchema();
        StreamingQuery query =
                spark.sql("select * from result" )
                        .writeStream().outputMode("append").option("truncate", false)
                        .format("console").trigger(Trigger.ProcessingTime(4000)).start();





        query.awaitTermination();

    }

}
