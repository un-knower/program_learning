package app;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

/**
 * 写入kafka
 */
class Data {
    private String from = "";
    private String devId;
    private String n;
    private String timestamp;
    private String kv;

    public Data(String from, String devId, String n, String timestamp, String kv) {
        this.from = from;
        this.devId = devId;
        this.n = n;
        this.timestamp = timestamp;
        this.kv = kv;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getDevId() {
        return devId;
    }

    public void setDevId(String devId) {
        this.devId = devId;
    }

    public String getN() {
        return n;
    }

    public void setN(String n) {
        this.n = n;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getKv() {
        return kv;
    }

    public void setKv(String kv) {
        this.kv = kv;
    }
}
public class OnlineLearningData2Kafka {
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
        Random random = new Random();

        String[] docs = {"doc1","doc2","doc3","doc4","doc4","doc5"};
        HashMap<String, String> hashMap = new HashMap<>();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while(true) {
            String n = "";
            hashMap.clear();
            if (random.nextBoolean()) {
                n = "RCC";
                hashMap.put("id", docs[random.nextInt(100)%docs.length]);
            } else {
                n = "EV";
                hashMap.put("ids", docs[random.nextInt(100)%docs.length]);
            }
            //Data data = new Data("2S5Wcx", "device"+random.nextInt(5), n, simpleDateFormat.format(System.currentTimeMillis()),JSON.toJSONString(hashMap));
            Data data = new Data("2S5Wcx", "device1", n, simpleDateFormat.format(System.currentTimeMillis()),JSON.toJSONString(hashMap));

            System.out.println(JSON.toJSON(data));
            str2Kafka(producer, JSON.toJSONString(data) );

            Thread.sleep(1000);

        }
    }
}
