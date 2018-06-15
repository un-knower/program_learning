package demo.kafka.demo3;

import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class KafkaProducer {
	public static void testProducer() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "192.168.1.1:9092");
        props.put("serializer.class", StringEncoder.class.getName());
        //props.put("partitioner.class", );
        props.put("request.required.arks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        String msg = new Date() + " - hello world : 测试 " ;
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", msg);
        producer.send(data);
        producer.close();
        System.out.println("--> producer sended： " + msg);
    }

    public static void main(String[] args) {
        testProducer();
    }
}
