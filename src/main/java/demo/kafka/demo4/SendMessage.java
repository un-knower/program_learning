package demo.kafka.demo4;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SendMessage {
	public static void main(String args[]) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "SparkMaster:2181,SparkWorker1:2181,SparkWorker2:2181/kafka");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		props.put("compression.codec", "1");
		props.put("metadata.broker.list", "SparkMaster:9092");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		///发送消息的格式
		//id memberid totalPrice youhui sendpay createdate
		Random random = new Random();
		for(int i=0;i<10;i++) {
			int id = random.nextInt(100000000);
			int memberid = random.nextInt(3);
			int totalPrice = random.nextInt(1000)+10;
			int youhui = random.nextInt(totalPrice);
			int sendpay = random.nextInt(3); //0 1 2
			
			StringBuffer data = new StringBuffer();
			data.append(String.valueOf(id))
				.append("\t")
				.append(String.valueOf(memberid))
				.append("\t")
				.append(String.valueOf(totalPrice))
				.append("\t")
				.append(String.valueOf(youhui))
				.append("\t")
				.append(String.valueOf(sendpay))
				.append("\t")
				.append("2014-04-09");
			System.out.println(data.toString());
			//发送数据                                                                                                                             topic      message  
			producer.send(new KeyedMessage<String, String>("order", data.toString()));
			
		}
		producer.close();
		System.out.println("send over...");
		
	}
}
