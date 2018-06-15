package demo.storm.kafka_storm.demo1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

public class KafkaConsumer {
	
	public static final String topic = "test";

	public static void main(String[] args) {
    	
        Properties props = new Properties(); //配置
        props.put("zookeeper.connect", "SparkMaster:2181,SparkWorker1:2181,SparkWorker2:2181/kafka");
        //group 代表一个消费组，每个消费者都要有个组
        props.put("group.id", "group2"); //每个消费组会从头开始消费。如果开启两个同为group1的消费者进程，那么数据来了其中一个进程消费，下一条数据来了另一个进程消费，即协助消费数据。如果两个进程一个是group1，另一个是group2，那么一条数据来了，都会接收到该数据
        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props); //将配置封装成ConsumerConfig
        ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config); //将ConsumerConfig代入ConsumerConnector

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        
        topicCountMap.put(topic, new Integer(1)); ////1个KafkaStream进行消费

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties()); //key的解析
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());  //value的解析

        Map<String, List<KafkaStream<String, String>>> consumerMap = 
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        //Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0); //取第一个KafkaStream流
        ConsumerIterator<String, String> it = stream.iterator();
        
        
        while (it.hasNext()) {
        	System.out.println(it.next().message());
        }
		 
	}
}
