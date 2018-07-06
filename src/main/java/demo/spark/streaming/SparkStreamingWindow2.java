package demo.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.*;

/**
 * @author wguangliang
 *
 * Durations.seconds(30), Durations.seconds(20) 有问题
 * Durations.seconds(30), Durations.seconds(10) 没问题
 *
 */
public class SparkStreamingWindow2 {
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
//
        window.print();
//        JavaDStream<Long> count = window.count();
//        count.print();

        // 启动
        jssc.start();
        jssc.awaitTermination();


    }

}
