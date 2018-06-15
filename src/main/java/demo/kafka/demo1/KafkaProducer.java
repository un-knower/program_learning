package demo.kafka.demo1;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;



import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;


public class KafkaProducer extends Thread{
	private String topic;
	public KafkaProducer(String topic) {
		this.topic = topic;
	}
	@Override
	public void run() {
		Producer<String, String> producer = createProducer();
		List<KeyedMessage<String, String>> msgList = new ArrayList<>();
		int i =0;
		try{
		while(true) {
			for(int j=0;j<2;j++){
				i++;
				msgList.add(new KeyedMessage<String, String>(topic, "message:"+i));
			}
			producer.send(msgList);
			
			msgList.clear();
			Thread.sleep(2000); //休息2s
			
		}
		}catch(Exception e) {
			if(producer!=null) {
				producer.close();
			}
		}
		
		
	}
	
	private Producer<String,String> createProducer() {
		Properties properties = new Properties();
		//声明zk
		properties.put("zookeeper.connect", "SparkMaster:2181,SparkWorker1:2181,SparkWorker2:2181");
		properties.put("serializer.class", StringEncoder.class.getName());
		properties.put("metadata.broker.list", "SparkMaster:9092");
		
		return new Producer<String,String>(new ProducerConfig(properties));
	}
	
	public static void main(String[] args) {
		new KafkaProducer("test").start();
	}
}
