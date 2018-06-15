package demo.storm.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
/**
 * trident的 ack处理
 * A non-transactional topology
 * 
 * Let's understand how we can write a non-transactional spout by implementing the
storm.trident.spout.IBatchSpout interface:
 * 
 * 
 * @author qingjian
 *
 */
public class FakeTweetSpout implements IBatchSpout{
	
	private static final long serialVersionUID = 10L;
	//批处理大小
	private int batchSize;
	//容器
	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();  //暂存批数据，用于重发数据
	
	public FakeTweetSpout(int batchSize) {
		this.batchSize = batchSize;
	}
	
	private static final Map<Integer, String> TWEET_MAP = new HashMap<Integer, String>();
	static {
		TWEET_MAP.put(0, " Adidas #FIFA World Cup Chant Challenge ");
		TWEET_MAP.put(1, "#FIFA worldcup");
		TWEET_MAP.put(2, "#FIFA worldcup");
		TWEET_MAP.put(3, " The Great Gatsby is such a good #movie ");
		TWEET_MAP.put(4, "#Movie top 10");
	}

	
	private static final Map<Integer, String> COUNTRY_MAP = new HashMap<Integer, String>();
	static {
		COUNTRY_MAP.put(0, "United State");
		COUNTRY_MAP.put(1, "Japan");
		COUNTRY_MAP.put(2, "India");
		COUNTRY_MAP.put(3, "China");
		COUNTRY_MAP.put(4, "Brazil");
	}
	/**
	 * 消息生成器
	 * @return TWEET信息和COUNTRY信息
	 */
	private List<Object> recordGenerator() {
		final Random rand = new Random();
		int randomNumber = rand.nextInt(5);
		int randomNumber2 = rand.nextInt(5);
		return new Values(TWEET_MAP.get(randomNumber),COUNTRY_MAP.get(randomNumber2));
	}
	
	public void ack(long batchId) {
		this.batchesMap.remove(batchId);  //如果确认了，则删除该批数据
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	/**
	 * 没有fail方法
	 */
	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batches = this.batchesMap.get(batchId); //得到批id
		if(batches == null) { //如果为空
			batches = new ArrayList<List<Object>>(); //初始化一个空队列
			for (int i=0;i < this.batchSize;i++) {
				batches.add(this.recordGenerator());  //队列中加入数据
			}
			this.batchesMap.put(batchId, batches);  //将发送的数据暂存
		}
		for(List<Object> list : batches){
		    try {
          System.out.println(new ObjectMapper().writeValueAsString(list));
        } catch (JsonProcessingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
            collector.emit(list);  //发送数据
        }
	}

	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("text","Country");  //定义输出的两个field
	}

	public void open(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		
	}

}
