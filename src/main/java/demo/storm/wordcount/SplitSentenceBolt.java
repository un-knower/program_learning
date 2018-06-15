package demo.storm.wordcount;

import java.util.StringTokenizer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//接收到一个句子
		String sentence = input.getString(0);
		//把句子分隔成单词
		StringTokenizer iter = new StringTokenizer(sentence);
		while(iter.hasMoreElements()) {
			collector.emit(new Values(iter.nextToken()));
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//定义各个字段Field
		declarer.declare(new Fields("word"));
	}

}
