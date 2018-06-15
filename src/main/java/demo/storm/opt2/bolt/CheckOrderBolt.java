package demo.storm.opt2.bolt;

import demo.storm.opt2.utils.DateUtils;
import org.apache.commons.lang.StringUtils;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 第一个bolt，检查订单是否有效，根据message里面的日期字段进行判断
 * createdate>=2014-04-19
 * @author qingjian
 *
 */
public class CheckOrderBolt extends BaseBasicBolt{

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String data = input.getString(0);
		if(data!=null && data.length()>0) {
			String[] values = data.split("\t");
			//订单号	用户id		原金额			优惠价		标示字段	下单时间
			//id	memberid	totalPirce	youhui	sendpay	createdate
			if(values.length==6) {
				String id = values[0];
				String memberid = values[1];
				String totalprice = values[2];
				String youhui = values[3];
				String sendpay = values[4];
				String createdate = values[5];
				
				if(StringUtils.isNotEmpty(id) &&
						StringUtils.isNotEmpty(memberid) &&
						StringUtils.isNotEmpty(totalprice) &&
						StringUtils.isNotEmpty(youhui) &&
						StringUtils.isNotEmpty(sendpay) &&
						StringUtils.isNotEmpty(createdate) ) {
					
					if(DateUtils.isDate(createdate, "2014-04-19")) { //如果是正确的信息
						collector.emit(new Values(id,memberid,totalprice,youhui,sendpay,createdate));
					}
					
				}
				
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","youhui","sendpay","createdate"));
	}
	
	//测试main
	public static void main(String args[]) {

		String data = "";
		if(data!=null && data.length()>0) {
			String[] values = data.split("\t");
			//订单号	用户id		原金额			优惠价		标示字段	下单时间
			//id	memberid	totalPirce	youhui	sendpay	createdate
			if(values.length==6) {
				String id = values[0];
				String memberid = values[1];
				String totalprice = values[2];
				String youhui = values[3];
				String sendpay = values[4];
				String createdate = values[5];
				
				if(StringUtils.isNotEmpty(id) &&
						StringUtils.isNotEmpty(memberid) &&
						StringUtils.isNotEmpty(totalprice) &&
						StringUtils.isNotEmpty(youhui) &&
						StringUtils.isNotEmpty(sendpay) &&
						StringUtils.isNotEmpty(createdate) ) {
					
					if(DateUtils.isDate(createdate, "2014-04-19")) { //如果是正确的信息
						System.out.println("true emit");
					} else {
						System.out.println("false");
					}
					
				}
				
			}
		}
	}

}
