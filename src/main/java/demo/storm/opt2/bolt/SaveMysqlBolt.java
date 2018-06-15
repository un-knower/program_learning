package demo.storm.opt2.bolt;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import demo.storm.opt2.utils.MemcachedUtil;
import org.apache.commons.lang.StringUtils;

import com.whalin.MemCached.MemCachedClient;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * 
 * count(id)
 * sum(totalprice)
 * sum(totalprice-youhui)
 * count(distinct memberid)
 * 
 * @author qingjian
 *
 */
public class SaveMysqlBolt extends BaseBasicBolt{

	private static Map<String, String> memberMap = null; //sendpay counterMember
	private static MemCachedClient memClient = null;
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		memberMap = new HashMap<String, String>();
		memClient = MemcachedUtil.getInstance(); //初始化memcached
		//定时进行入库
		Timer timer = new Timer();
		timer.schedule(new cacheTimer(), new Date(), 5000);
		
		
	}

	class cacheTimer extends TimerTask {

		@Override
		public void run() {
			Map<String, String> tmpMap = new HashMap<String, String>();
			tmpMap.putAll(memberMap);
			memberMap = new HashMap<String, String>();
			
			saveMysql(tmpMap);
					
			
		}
		
	}
	
	private void saveMysql(Map<String, String> tmpMap) {
		try{
			for (Map.Entry<String, String> entry:tmpMap.entrySet()) {
				//id,order_nums,p_total_price,y_total_price,order_members,sendpay
				String key = entry.getKey();
				String value = entry.getValue();
				String[] sendpayInfoSplit = value.split(",");
				int count_id = Integer.parseInt(sendpayInfoSplit[0]);
				double totalprice = Double.valueOf(sendpayInfoSplit[1]);
				double youhui = Double.valueOf(sendpayInfoSplit[2]);
				int counter_member = Integer.parseInt(sendpayInfoSplit[3]);
				
				
				
			}
		}catch(Exception e) {
			
			
			
		}
	}
	
	/**
	 * 记录独立用户数
	 * @param memberid
	 * @param sendpay
	 */
	private void saveCounterMember(String memberid,String sendpay,String totalprice, String youhui) {
		try {
			String key = sendpay+"_"+memberid; //在memcached中以sendpay_memberid两个维度存储
			String vx = (String) memClient.get(key);
			String sendpayInfo = memberMap.get(sendpay); //value = count(id),sum(totalprice),sum(totalprice-youhui),count(distinct memberid)
			if(sendpayInfo!=null) {
				String[] sendpayInfoSplit = sendpayInfo.split(",");
				int count_id = Integer.parseInt(sendpayInfoSplit[0]) + 1;
				double sum_totalprice = Double.valueOf(sendpayInfoSplit[1]) + Double.valueOf(totalprice);
				double sum_totalprice_youhui = sum_totalprice - Double.parseDouble(youhui);
				int count_distinctmem = Integer.parseInt(sendpayInfoSplit[3]);
				if(StringUtils.isNotEmpty(vx)) { //如果该memberid在该sendpay维度下存在
					count_distinctmem +=1; 
				}
				sendpayInfo = count_id+","+sum_totalprice+","+sum_totalprice_youhui+","+count_distinctmem;
			}
			else { //如果不存在
				sendpayInfo = 1+","+totalprice+","+(Double.parseDouble(totalprice)-Double.parseDouble(youhui))+","+1;
			}
			System.out.println("...........sendpay = "+sendpay+","+sendpayInfo);
			memberMap.put(sendpay, sendpayInfo);
			
			
		} catch (Exception e) {
		}
		
	}
	
	
	
	
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		List<Object> values = input.getValues();
		String id = (String) values.get(0);
		String memberid = (String) values.get(1);
		String totalprice = (String) values.get(2);
		String youhui = (String) values.get(3);
		String sendpay = (String) values.get(4);
		String createdate = (String) values.get(5);
		
		saveCounterMember(memberid,sendpay,totalprice,youhui); //记录独立用户数
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
