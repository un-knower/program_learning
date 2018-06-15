package demo.storm.distribution;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;



public class PWTopology3 {

	public static void main(String[] args) throws Exception {
		//
		Config cfg = new Config();
		cfg.setNumWorkers(2);
		cfg.setDebug(true);
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new PWSpout(), 4);
		builder.setBolt("print-bolt", new PrintBolt(), 4).shuffleGrouping("spout");
		//设置字段分组
		//builder.setBolt("write-bolt", new WriteBolt(), 8).fieldsGrouping("print-bolt", new Fields("write"));//不一定会生成多少个文件
		//设置广播分组
		builder.setBolt("write-bolt", new WriteBolt(), 5).allGrouping("print-bolt"); //生成并行度个文件
		//设置全局分组
		//builder.setBolt("write-bolt", new WriteBolt(), 4).globalGrouping("print-bolt"); //只会生成一个文件

		//1 本地模式
		cfg.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("top3", cfg, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("top3");
		cluster.shutdown();
		
		//2 集群模式
//		StormSubmitter.submitTopology("top3", cfg, builder.createTopology());
		
	}
}
