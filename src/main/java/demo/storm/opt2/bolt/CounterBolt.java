package demo.storm.opt2.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CounterBolt extends BaseBasicBolt {
	private static long counter = 0;
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("message = "+input.getString(0)+"  counter = "+counter++);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
