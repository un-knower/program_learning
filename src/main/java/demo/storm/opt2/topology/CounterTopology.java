package demo.storm.opt2.topology;


import demo.storm.opt2.bolt.CounterBolt;
import org.apache.storm.guava.collect.ImmutableList;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * 执行 
 * 在nimbus进程的机器上执行
 * bin/storm jar Storm-Kafka-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.kafka.storm.opt2.topology.CounterTopology
 * @author qingjian
 *
 */
public class CounterTopology {
	public static void main(String args[]) {
		try {
			
			//kafka 在zookeeper的路径
			String kafkaZookeeper = "SparkMaster:2181,SparkWorker1:2181,SparkWorker2:2181/kafka";
			BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper); 
			
			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "order", "/order", "id"); //需要在zookeeper中创建路径  /order   /order/id    使用命令  create /order 1   create /order/id 1
			kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
			kafkaConfig.zkServers = ImmutableList.of("SparkMaster","SparkWorker1","SparkWorker2");
			kafkaConfig.zkPort = 2181;
			kafkaConfig.forceFromStart = true;  //强制从开始读数据
			
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("spout", new KafkaSpout(kafkaConfig), 2); //2个线程
			builder.setBolt("counter", new CounterBolt(),1).shuffleGrouping("spout");
			
			Config config = new Config(); //Storm配置
			config.setDebug(true);
			
			if(args!=null && args.length>0) {
				config.setNumWorkers(2);
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			}
			else {
				config.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("special-topology", config, builder.createTopology());
				
				Thread.sleep(500000);
				
				cluster.shutdown();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		
		
		
		
		
		
	}
}
