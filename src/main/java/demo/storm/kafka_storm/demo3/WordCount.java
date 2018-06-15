package demo.storm.kafka_storm.demo3;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 
 * 打包 jar  storm-kafka_wordcount.jar
 * 提交到服务器，运行 
 * storm jar storm-kafka_wordcount.jar demo.kafka_storm.WordCount SparkMaster
 * storm jar Storm-Kafka-0.0.1-SNAPSHOT.jar demo.kafka_storm.WordCount SparkMaster
 * @author qingjian
 *
 */
public class WordCount {
	public static class KafkaWordSplitter extends BaseRichBolt {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);
		
		private OutputCollector collector;
		public void execute(Tuple input) {
			String line = input.getString(0);
			LOG.info("RECV[kafka -> splitter] "+line);
			String[] words = line.split("\\s+");
			for (String word : words) {
				LOG.info("EMIT[splitter -> counter] "+word);
				collector.emit(input, new Values(word,1));
			}
			
			collector.ack(input); //确认
		}

		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word","count"));
		}
		
	}//class
	
	public static class WordCounter extends BaseRichBolt {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(WordCounter.class);
		private OutputCollector collector;
		private Map<String, AtomicInteger> counterMap; //AtomicInteger是线程安全的，原子操作
		
		
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
			this.counterMap = new HashMap<String, AtomicInteger>();
			
		}

		
		public void execute(Tuple input) {
			String word = input.getString(0);
			int count = input.getInteger(1);
			LOG.info("EMIT[splitter -> counter] "+word+":"+count);
			AtomicInteger atomicInteger = this.counterMap.get(word);
			if(atomicInteger==null) { //如果不存在该word
				atomicInteger = new AtomicInteger(); //0
				counterMap.put(word, atomicInteger);
			}
			atomicInteger.addAndGet(count);
			collector.ack(input); //确认
			LOG.info("CHECK statistics map: " + this.counterMap);
		}

		
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word","count"));
		}

		
		public void cleanup() { //展示最后的统计结果
			LOG.info("The final result:");
			Iterator<Entry<String, AtomicInteger>> iterator = this.counterMap.entrySet().iterator();
			while(iterator.hasNext()) {
				Entry<String, AtomicInteger> entry = iterator.next();
				LOG.info(entry.getKey()+"\t:\t"+entry.getValue());
			}
			
		}
		
	}//class
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		String zks = "SparkMaster:2181,SparkWorker1:2181,SparkWorker2:2181";
		String topic = "test";
		String zkRoot = "/storm"; //// default zookeeper root configuration for storm
		String id = "word";
		/*定义kafka spout*/
//		BrokerHosts brokerHosts = new ZkHosts(zks);
		//因为默认kafka在zookeeper的根目录下 /brokers
		//但是集群中修改了位置 zookeeper.connect=SparkMaster:2181,SparkWorker1:2181,SparkWorker2:2181/kafka
		//所以需要设置   new ZkHosts(zks,"/kafka/brokers");
		ZkHosts brokerHosts = new ZkHosts(zks,"/kafka/brokers");
		//第一个参数broker地址 。 第二个参数topic  第三个参数Third argument is the zookeeper root for Kafka。默认是/storm 第四个参数是消费者的group.id。Fourth argument is consumer group id
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		//指定kafka的消息为String类型的
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.forceFromStart=false; //是否从头开始消费
		spoutConfig.zkServers = Arrays.asList(new String[]{"SparkMaster","SparkWorker1","SparkWorker2"});
		spoutConfig.zkPort=2181;
		/*定义Topology*/
		TopologyBuilder builder = new TopologyBuilder();
		//设置spout
		builder.setSpout("kafka-reader", new KafkaSpout(spoutConfig),2);// Kafka我们创建了一个2分区的Topic，这里并行度设置为2
		//设置bolt
		builder.setBolt("word-splitter", new KafkaWordSplitter(),2)
			.shuffleGrouping("kafka-reader");
		builder.setBolt("word-counter", new WordCounter())
			.fieldsGrouping("word-splitter", new Fields("word")); //按照word分组
		/*定义Storm Config*/
		Config conf = new Config(); //storm config
		String name = WordCount.class.getSimpleName();
		/*提交作业*/
		if(args!=null && args.length>0) {
			conf.put(Config.NIMBUS_HOST, args[0]);
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(name, conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(60000);
			cluster.shutdown();
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
