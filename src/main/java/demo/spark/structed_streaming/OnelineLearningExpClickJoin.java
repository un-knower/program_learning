package demo.spark.structed_streaming;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import scala.Tuple4;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author wguangliang
 */
public class OnelineLearningExpClickJoin {
    private static final String bootstrapServers = "SparkMaster:9092";
    private static final String topics = "flinktest";  //topic1,topic2
    public static void main(String args[]) throws InterruptedException, StreamingQueryException {
        // thrift请求体
        // FeatureExtractServiceRequest request = new FeatureExtractServiceRequest();

        StructType structType = new StructType()
                .add("devId","string")
                .add("time","timestamp")
                .add("type","string")
                .add("doc","string");



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

        Dataset<Row> touTiaoData = lines.repartition(3).filter(new FilterFunction<String>() {
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
//                            System.out.println("flatmap:"+line);
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
                }, Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP(), Encoders.STRING(), Encoders.STRING())).toDF("deviceid", "time", "event", "doc")
                .withWatermark("time", "14 seconds");  //water mark 14s
//                .withWatermark("time", "10 hours");  //water mark 14s
//                .withWatermark("deviceid", "4 seconds")  //water mark 14s
//                .withWatermark("doc", "4 seconds");  //water mark 14s


        touTiaoData.createOrReplaceTempView("online");


        Dataset<Row> evDataSet = spark.sql("select * from online where event='EV'").withWatermark("time", "4 seconds");
        evDataSet.createOrReplaceTempView("ev");
        Dataset<Row> clickDataSet = spark.sql("select * from online where event='RCC'").withWatermark("time", "4 seconds");
        clickDataSet.createOrReplaceTempView("click");


        StreamingQuery query =
//                spark.sql("select ev.deviceid,ev.time as time, if(click.doc is null,0,1) " +
//                        "from " +
//                        "( " +
//                        "select * " +
//                        "from online " +
//                        "where event='EV' " +
//                        ")ev inner join " +
//                        "( " +
//                        "select * " +
//                        "from online " +
//                        "where event='RCC' " +
//                        ") click " +
//                        "on ev.deviceid=click.deviceid and ev.doc = click.doc and click.time >= ev.time and click.time < ev.time + INTERVAL 14 seconds " +
//                        "where (unix_timestamp(click.time)-unix_timestamp(ev.time)<=14) or click.doc is null"   //窗口长度+移动长度=14s
//                )
                  spark.sql("select ev.deviceid,ev.time as time, if(click.doc is null,0,1) " +
                        "from " +
                        "( " +
                        "select * " +
                        "from online " +
                        "where event='EV' " +
                        ")ev inner join " +
                        "( " +
                        "select * " +
                        "from online " +
                        "where event='RCC' " +
                        ") click " +
                        "on ev.deviceid=click.deviceid and ev.doc = click.doc and click.time >= ev.time and click.time < ev.time + INTERVAL 14 seconds "
                        //"where (unix_timestamp(click.time)-unix_timestamp(ev.time)<=14) or click.doc is null"   //窗口长度+移动长度=14s
                  )
//                spark.sql("select *,if(click.doc is null,0,1) from ev left join click on ev.deviceid=click.deviceid and ev.doc = click.doc and click.time >= ev.time and click.time <= ev.time + INTERVAL 14 seconds ")
//                spark.sql("select *,if(click.doc is null,0,1) from ev left outer join click on ev.deviceid=click.deviceid and ev.doc = click.doc and click.time >= ev.time and click.time <= ev.time + INTERVAL 14 seconds")
//                spark.sql("select * from online")
                .writeStream().outputMode("append").option("truncate", false)
                .format("console").trigger(Trigger.ProcessingTime(4000)).start();

        query.awaitTermination();

    }

}
