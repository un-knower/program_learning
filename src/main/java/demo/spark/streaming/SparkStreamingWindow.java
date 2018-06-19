package demo.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * @author bjwangguangliang
 *
 * Durations.seconds(30), Durations.seconds(20) 有问题
 * Durations.seconds(30), Durations.seconds(10) 没问题
 *
 */
public class SparkStreamingWindow {
    private static final String topics = "flinktest";

    public static void main(String args[]) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[3]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

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

        // kafka producer configuration
        final Broadcast<String> brokerListBroadcast = jssc.sparkContext().broadcast("SparkMaster:9092");
        final Broadcast<String> topicBroadcast = jssc.sparkContext().broadcast("flinkout");


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

        JavaPairDStream<String, Integer> wordSplitWindow = dataSource.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(":")[0];
//                return s;
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {

                return new Tuple2<>(word, 1);
            }
        }).window(Durations.seconds(30), Durations.seconds(15));

        JavaDStream<Long> count = wordSplitWindow.count();


//        wordSplit.cache();

//        JavaPairDStream<String, Integer> window1 = wordSplit.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer num1, Integer num2) throws Exception {
//                return num1 + num2;
//            }
//        }, Durations.seconds(20), Durations.seconds(20));




//        window1.print();

        count.print();
//        count.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
//            @Override
//            public void call(JavaPairRDD<String, Integer> winRdd) throws Exception {
//                winRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
//                    @Override
//                    public void call(Iterator<Tuple2<String, Integer>> stringIterator) throws Exception {
//                        KafkaProducer kafkaProducer = KafkaProducer.getInstance(brokerListBroadcast.getValue());
//                        // 批量发送 推荐
//                        List<KeyedMessage<String, String>> messageList = Lists.newArrayList();
//                        while (stringIterator.hasNext()) {
//                            Tuple2<String, Integer> next = stringIterator.next();
//
//                            messageList.add(new KeyedMessage<String, String>(topicBroadcast.getValue(), next.toString()));
//                        }
//                        kafkaProducer.send(messageList);
//                    }
//                });
//            }
//        });


        // 启动
        jssc.start();
        jssc.awaitTermination();


    }

}
