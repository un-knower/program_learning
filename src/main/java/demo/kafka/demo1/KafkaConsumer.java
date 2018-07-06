package demo.kafka.demo1;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


public class KafkaConsumer extends Thread{
	private String topic;
	
	public KafkaConsumer(String topic) {
		this.topic = topic;
	}


	@Override
	public void run() {
		ConsumerConnector consumer = createConsumer();
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1); //1个KafkaStream进行消费
		Map<String, List<KafkaStream<byte[], byte[]>>> messageStream = consumer.createMessageStreams(topicCountMap);//a map of (topic, list of  KafkaStream) pairs.a map of (topic, #streams) pair
		KafkaStream<byte[], byte[]> kafkaStream = messageStream.get(topic).get(0);// 获取每次接收到的这个数据  
		
		ConsumerIterator<byte[], byte[]> iterator = kafkaStream.iterator();
		while(iterator.hasNext()) {
			String message = new String(iterator.next().message());
			System.out.println("接收到数据："+message);
		}
	}
	
	private ConsumerConnector createConsumer() {
		Properties properties= new Properties();
		//声明zk
		properties.put("zookeeper.connect", "SparkMaster:2181,SparkWorker1:2181,SparkWorker2:2181/kafka");
		properties.put("group.id", "group1");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
//		props.put("zookeeper.session.timeout.ms", "60000");  
//        props.put("zookeeper.sync.time.ms", "2000");  
//        props.put("auto.commit.interval.ms", "1000");  //props.put("auto.commit.enable", "true");// 默认为true，让consumer定期commit offset，zookeeper会将offset写入到文件中，否则只在内存，若故障则再消费时会重头开始
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
	}
	public static void main(String[] args) {
		new KafkaConsumer("datacenter_sdk_etl").start();
	}

}
