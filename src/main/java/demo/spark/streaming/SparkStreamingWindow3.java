package demo.spark.streaming;

import com.alibaba.fastjson.JSONObject;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.sql.Timestamp;
import java.util.*;

/**
 * @author bjwangguangliang
 *
 * Durations.seconds(30), Durations.seconds(20) 有问题
 * Durations.seconds(30), Durations.seconds(10) 没问题
 *
 */
class SQLContextSingleton {
    transient  private static SQLContext instance = null;

    public static SQLContext getInstance(SparkContext sc) {
        if (instance == null) {
            instance = new SQLContext(sc);
        }
        return instance;
    }
}

public class SparkStreamingWindow3 {
    private static final String topics = "flinktest";

    public static void main(String args[]) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
        jssc.checkpoint(".");
        // kafka consumer configuration
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        String brokers = "SparkMaster:9092";
        kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("deviceid", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("time", DataTypes.TimestampType, true));
        structFields.add(DataTypes.createStructField("event", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("doc", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);
        JavaPairInputDStream<String, String> kafkaDataSource = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );





        JavaDStream<String> dataSource = kafkaDataSource.map(line -> line._2);

//
        JavaDStream<String> window = dataSource.window(Durations.seconds(10), Durations.seconds(4));
        window.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> rdd, Time time) throws Exception {
                SQLContext instance = SQLContextSingleton.getInstance(rdd.context());
                JavaRDD<Row> touTiaoRdd = rdd.filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String line) throws Exception {
                        return (StringUtils.contains(line, "2x1kfBk63z") || StringUtils.contains(line, "2S5Wcx")) &&
                                (StringUtils.contains(line, "EV") || StringUtils.contains(line, "RCC"));
                    }
                }).flatMap(new FlatMapFunction<String, Row>() {
                    List<Row> result = new ArrayList<>();

                    @Override
                    public Iterator<Row> call(String line) throws Exception {
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
                                    result.add(RowFactory.create(devId, timestamp, event, docId));
                                }
                            }

                        } else if ("RCC".equals(event)) {
                            String kv = jsonObject.getString("kv");
                            if (kv != null) {
                                String docId = JSONObject.parseObject(kv).getString("id");
                                result.add(RowFactory.create(devId, timestamp, event, docId));
                            }
                        }

                        return result.iterator();


                    }
                });

                Dataset<Row> dataFrame = instance.createDataFrame(touTiaoRdd, structType);
                dataFrame.registerTempTable("online");
                Dataset<Row> result = instance.sql("select *,if(click.doc is null,0,1) from (select * from online where event='EV')ev left outer join (select * from online where event='RCC')click on ev.deviceid=click.deviceid and ev.doc = click.doc and click.time >= ev.time and click.time <= ev.time + INTERVAL 10 seconds ");
                result.show();
                /**
                 * 第六步：对结果进行处理，包括由DataFrame转换成为RDD<Row>,以及结果的持久化
                 */
//                List<Row> listRow = result.javaRDD().collect();
//               collect for(Row row : listRow){
//                    System.out.println(row);
//                }

            }
        });



//
        window.print();
//        JavaDStream<Long> count = window.count();
//        count.print();

        // 启动
        jssc.start();
        jssc.awaitTermination();


    }

}
