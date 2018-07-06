package demo.spark.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;
import parquet.Preconditions;

import java.util.List;
import java.util.Properties;

/**
 * @author wguangliang
 */
public class KafkaProducer implements KryoSerializable {
    public static final String METADATA_BROKER_LIST_KEY = "metadata.broker.list";
    public static final String SERIALIZER_CLASS_KEY = "serializer.class";
    public static final String SERIALIZER_CLASS_VALUE = "kafka.serializer.StringEncoder";

    private static KafkaProducer instance = null;
    private Producer producer;
    private String brokerList;
    private KafkaProducer(String brokerList) {
        System.out.println("initial kafka " + brokerList);
        Preconditions.checkArgument(StringUtils.isNotBlank(brokerList), "kafka brokerlist is blank ...");
        // set properties
        this.brokerList = brokerList;
        Properties properties = new Properties();
        properties.put(METADATA_BROKER_LIST_KEY, brokerList);
        properties.put(SERIALIZER_CLASS_KEY, SERIALIZER_CLASS_VALUE);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.producer = new Producer(producerConfig);
    }

    public static synchronized KafkaProducer getInstance(String brokerList) {
        if (instance == null) {
            Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
            instance = new KafkaProducer(brokerList);
        }
        return instance;
    }
    static class CleanWorkThread extends Thread {
        @Override
        public void run() {
            System.out.println("Destroy Redis Cluster");
            if (instance != null) {
                instance.producer.close();
            }

        }
    }

    // 单条发送
    public void send(KeyedMessage<String, String> keyedMessage) {
        producer.send(keyedMessage);
    }

    // 批量发送
    public void send(List<KeyedMessage<String, String>> keyedMessageList) {
        producer.send(keyedMessageList);
    }


    public void shutdown() {
        producer.close();
    }


    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, this.brokerList);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        String brokerList = kryo.readObject(input, String.class);
        Preconditions.checkArgument(StringUtils.isNotBlank(brokerList), "kafka brokerlist is blank ...");
        // set properties
        this.brokerList = brokerList;
        Properties properties = new Properties();
        properties.put(METADATA_BROKER_LIST_KEY, brokerList);
        properties.put(SERIALIZER_CLASS_KEY, SERIALIZER_CLASS_VALUE);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.producer = new Producer(producerConfig);

    }
}
