package demo.storm.opt2.bolt;

import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 
 * 进行sendpay的数据纠正
 * sendpay如果为'1'，转换为1
 * sendpay如果为'2'，转换为2
 * sendpay如果为其他，转换为-1
 * 
 * @author qingjian
 *
 */
public class TranslateBolt extends BaseBasicBolt{

	public void execute(Tuple input, BasicOutputCollector collector) {
		List<Object> values = input.getValues();
		String id = (String) values.get(0);
		String memberid = (String) values.get(1);
		String totalprice = (String) values.get(2);
		String youhui = (String) values.get(3);
		String sendpay = (String) values.get(4);
		String createdate = (String) values.get(5);
		
		if(!"1".equals(sendpay)||"2".equals(sendpay)) {
			sendpay="-1";
		}
		
		collector.emit(new Values(id,memberid,totalprice,youhui,sendpay,createdate));
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","youhui","sendpay","createdate"));
	}

}
