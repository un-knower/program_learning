package app;

import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.util.Properties;

/**
 * 写入kafka
 */
public class Data2Kafka {
    private static final String SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"; // 序列化类
    private static final String TOPIC = "flinktest"; //kafka创建的topic
    private static final String BROKER_LIST = "sparkmaster:9092"; //broker的地址和端口
    public static void str2Kafka(Producer<String, String> producer, String content) {
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, content);
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null) {
                    e.printStackTrace();
                }
                System.out.println("The offset of the record we just sent is: " + recordMetadata.offset());
            }
        });

    }
    public static void main(String args[]) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.put("key.serializer", SERIALIZER_CLASS);
        props.put("value.serializer", SERIALIZER_CLASS);
        props.put("bootstrap.servers", BROKER_LIST);

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        int num = 1;
        String[] chars = {"a","b","c","d","e","f"};
        while(true) {
            str2Kafka(producer, chars[num%chars.length]+":"+System.currentTimeMillis() );

            Thread.sleep(1000);

            num += 1;
        }
    }
}
