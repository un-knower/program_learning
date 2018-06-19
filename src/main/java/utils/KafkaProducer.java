package utils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;
import parquet.Preconditions;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * @author bjwangguangliang
 */
public class KafkaProducer implements Serializable {
    public static final String METADATA_BROKER_LIST_KEY = "metadata.broker.list";
    public static final String SERIALIZER_CLASS_KEY = "serializer.class";
    public static final String SERIALIZER_CLASS_VALUE = "kafka.serializer.StringEncoder";

    private static KafkaProducer instance = null;
    private Producer producer;

    private KafkaProducer(String brokerList) {
        Preconditions.checkArgument(StringUtils.isNotBlank(brokerList), "kafka brokerlist is blank ...");
        // set properties
        Properties properties = new Properties();
        properties.put(METADATA_BROKER_LIST_KEY, brokerList);
        properties.put(SERIALIZER_CLASS_KEY, SERIALIZER_CLASS_VALUE);
        ProducerConfig producerConfig = new ProducerConfig(properties);
        this.producer = new Producer(producerConfig);
    }

    public static synchronized KafkaProducer getInstance(String brokerList) {
        if (instance == null) {
            instance = new KafkaProducer(brokerList);
        }
        return instance;
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



}
