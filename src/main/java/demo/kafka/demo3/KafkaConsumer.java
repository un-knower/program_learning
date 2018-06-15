package demo.kafka.demo3;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer {

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public KafkaConsumer(String a_zookeeper, String a_groupId, String a_topic) {
        this.consumer = kafka.consumer.Consumer
                .createJavaConsumerConnector(createConsumerConfig(a_zookeeper,
                        a_groupId));

        this.topic = a_topic;
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper,
            String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "1000");
        props.put("zookeeper.sync.time.ms", "1000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        return new ConsumerConfig(props);
    }

    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();
        if (executor != null)
            executor.shutdown();
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        System.out.println("streams.size = " + streams.size());

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }

    public static void main(String[] args) {

        String zooKeeper = "192.168.212.100:2181";
        String groupId = "group1";
        String topic = "test";

        int threads = 3;

        KafkaConsumer example = new KafkaConsumer(zooKeeper, groupId, topic);

        example.run(threads);

    }

    public class ConsumerTest implements Runnable {

        private KafkaStream m_stream;
        private int m_threadNumber;

        public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
        }

        public void run() {
            System.out.println("calling ConsumerTest.run()");
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();

            while (it.hasNext()) {
                System.out.println("--> consumer  Thread " + m_threadNumber + ": "
                        + new String(it.next().message()));
            }

            System.out.println("Shutting down Thread: " + m_threadNumber);
        }
    }

}
