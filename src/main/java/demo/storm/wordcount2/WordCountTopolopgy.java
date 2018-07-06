package demo.storm.wordcount2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * 
 * 这种读取某个目录下文件的方式不好，会造成数据的重复读取！！！！
 * @author qingjian
 *
 */
public class WordCountTopolopgy {
	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Usage: inputPath timeOffset");
			System.err.println("./storm jar wordcount2.jar storm.demo.wordcount2.WordCountTopolopgy /home/wguangliang/storm/data 2");
			System.exit(2);
		}
		
		
		TopologyBuilder builder = new TopologyBuilder();
		//设置Spout，并行度为5
		builder.setSpout("spout", new WordReaderSpout(), 5);
		//设置bolt，并行度为8，他的数据来源是spout
		builder.setBolt("split", new SplitSentenceBolt(), 8).shuffleGrouping("spout");
		//设置bolt，并行度为12，他的数据来源是split,以word字段进行分组
		builder.setBolt("count", new WordCountBolt(), 12).fieldsGrouping("split", new Fields("word"));
		
		Config conf = new Config();
		conf.setDebug(false);
		
		String inputPath = args[0];
		String timeOffset = args[1];
		conf.put("INPUT_PATH", inputPath);
		conf.put("TIME_OFFSET", timeOffset);
		 
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("wordcount", conf, builder.createTopology());
	}
}
