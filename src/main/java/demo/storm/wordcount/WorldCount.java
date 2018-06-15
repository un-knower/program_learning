package demo.storm.wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WorldCount {
  class RandomSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Random random;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      this._collector = collector;
      this.random = new Random();
    }

    @Override
    public void nextTuple() {
      Utils.sleep(1000);
      
      String[] sentences = {
          "hello you",
          "hello me",
          "hello world",
          "you me"
      };
      String sentence = sentences[this.random.nextInt()]; 
      this._collector.emit(new Values(sentence));
      
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("sentence"));
    }
    
  }
  class SplitBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String sentence = input.getStringByField("sentence");
      StringTokenizer token = new StringTokenizer(sentence);
      while(token.hasMoreElements()) {
        collector.emit(new Values(token.nextElement()));
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
    
  }
  
  class CountBolt extends BaseBasicBolt {
    HashMap<String, Long> map = new HashMap<>();
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      String word = input.getString(0);
      Long count = map.get(word);
      if(count == null) {
        count = 0l;
      }
      count ++;
      
      map.put(word, count);
      System.out.println("[result]"+word+":"+count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      
    }
    
  }
  
  class CountTopology  {
    
  }
  
  
}
