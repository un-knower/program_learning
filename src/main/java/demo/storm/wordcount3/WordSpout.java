package demo.storm.wordcount3;

import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * Spout作为数据源，实现IRichSpout接口
 * 功能是读取一个文本文件，并把它的每一行内容发送给bolt
 * @author qingjian
 *
 */
public class WordSpout implements IRichSpout {

	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	 
	private int index = 0;
	
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
	
    /**
     * 这是第一个方法，里面接收了三个参数
     * 第一个参数是创建Topology时的配置
     * 第二个是所有的Topology数据
     * 第三个是用来把spout的数据发射给bolt
     */
	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForSeconds(1);	
	}
	

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
	@Override
	public void ack(Object msgId) {
	}
	
	@Override
	public void fail(Object msgId) {
	}

	@Override
	public void activate() {
		
	}

	@Override
	public void close() {
		
	}

	@Override
	public void deactivate() {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
