package demo.storm.wordcount;

import java.util.HashMap;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseBasicBolt {
	HashMap<String, Integer> counts = new HashMap<>();
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		//获取该单词对应的计数
		Integer count = counts.get(word);
		if(count == null) {
			count =0;
		}
		
		count ++;
		counts.put(word, count);
		System.out.println("[result]"+word+":"+count);
		collector.emit(new Values(word, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}
	
}
