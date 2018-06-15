package demo.storm.wordcount2;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class WordCountBolt extends BaseBasicBolt {
	HashMap<String, Integer> counters = new HashMap<>();
	private volatile boolean edit = false;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		final long timeOffset = Long.parseLong(stormConf.get("TIME_OFFSET").toString());
		//WordCounter类的prepare方法，里面定义了一个Thread，持续监控容器的变化（word个数增加或者新增word）。
		new Thread(new Runnable() {
			@Override
			public void run() {
				while(true) {
					if(edit) {
						for (Entry<String, Integer> entry : counters.entrySet()) {
							System.out.println(entry.getKey()+":"+entry.getValue());
						}
						System.out.println("WordCounter-------------");
						edit = false;
					}
					
					try {
						Thread.sleep(timeOffset*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();;
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		if(!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str);
			counters.put(str, c+1);
		}
		edit = true;
		System.out.println("WordCounter++++++++++++++++++++++");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}
	
}
