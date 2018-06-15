package demo.storm.drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This topology is a basic example of doing distributed RPC on top of Storm. It
 * implements a function that appends a "!" to any string you send the DRPC
 * function.
 * <p/>
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more
 * information on doing distributed RPC on top of Storm.
 */
public class BasicDRPCTopology {

	/**
	 * bolt
	 */
	public static class ExclaimBolt extends BaseBasicBolt {
		
		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {//注意这里传入的tuple格式是["request-id","args"]
			String input = tuple.getString(1); //tuple.getLong(0)是request-id
			System.out.println("==========" + tuple.getValue(0));//==========4213500786923817749 //==========-1609306047856287744
			System.out.println("tuple.getString(0):"+tuple.getLong(0));//tuple.getString(0):4213500786923817749 //tuple.getString(0):-1609306047856287744
			System.out.println("tuple.getString(1):"+tuple.getString(1));//tuple.getString(1):hello //tuple.getString(1):goodbye
			
			collector.emit(new Values(tuple.getValue(0), input + "!")); //注意这里要返回["request-id","result"]
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}
	}

	public static void main(String[] args) throws Exception {
		// 创建drpc实例
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation"); // drpc名字
		// 添加bolt
		builder.addBolt(new ExclaimBolt(), 3);
		Config conf = new Config();
		if (args == null || args.length == 0) { //本地测试
			LocalDRPC drpc = new LocalDRPC(); // 本地drpc
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));

			// DRPC一般是本地指定数据源，去调远程的服务
			for (String word : new String[] { "hello", "goodbye" }) {
				System.out.println("Result for \"" + word + "\": " + drpc.execute("exclamation", word));
			}

			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumWorkers(3);
			// 启动drpc命令 bin/storm drpc
			StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
		}
	}
}