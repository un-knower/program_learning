package demo.storm.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopolopgy {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		//设置Spout，并行度为5
		builder.setSpout("spout", new RandomSentenceSpout(), 5);
		//设置bolt，并行度为8，他的数据来源是spout
		builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
		//设置bolt，并行度为12，他的数据来源是split,以word字段进行分组
		builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(true);
		
		 
		if(args != null && args.length >0) {
			conf.setNumWorkers(3);
			//集群生产环境
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			//本机测试
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("word-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
		
		
	}
}
