package demo.storm.wordcount;

import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class RandomSentenceSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	Random _rand;
	private static final Log log = LogFactory.getLog(RandomSentenceSpout.class);
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		_rand = new Random();
	}

	@Override
	public void nextTuple() {
		//睡眠一段时间后再产生一个数据
		Utils.sleep(1000);
		
		//数据源
		String[] sentences = new String[] {
				"the cow jumped over the moon", 
				"an apple a day keeps the doctor away",  
                "four score and seven years ago", 
                "snow white and the seven dwarfs", 
                "i am at two with nature"	
		};
		//随机选择一个句子
		String sentence = sentences[_rand.nextInt(sentences.length)];
		log.info("[spout] "+sentence);
		//发送给bolt
		_collector.emit(new Values(sentence));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("words")); //给发送的tuple数据中的各个Field起名字
	}

}
