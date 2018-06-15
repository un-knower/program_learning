package demo.storm.trident;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class TridentHelloWorldTopology {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20); //这个设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复， 我们推荐你设置这个配置，以防止tuple队列爆掉。
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Count", conf, buildTopology());
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
	
	public static StormTopology buildTopology() {

		FakeTweetSpout spout = new FakeTweetSpout(10); //一组batch有10个信息
		TridentTopology topology = new TridentTopology();

		topology.newStream("spout1", spout)
		        .parallelismHint(2)  //设置spout的并行度partition为2
				.shuffle()  //spout经过shuffle传递下去
				.each(new Fields("text", "Country"),
						new TridentUtility.TweetFilter())
				.parallelismHint(5)  //设置filter的并行度为5
				.groupBy(new Fields("Country"))
				.aggregate(new Fields("Country"), new Count(),
						new Fields("count"))
				.each(new Fields("Country","count"), new TridentUtility.Print())
				.parallelismHint(2);

		return topology.build();
	}
}
