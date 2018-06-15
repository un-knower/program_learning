package demo.storm.drpc2;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This program creates a topology that can
 * compute the reach for any URL on Twitter in realtime by parallelizing the whole computation.
 * <p/>
 * Reach is the number of unique people exposed to a URL on Twitter. To compute reach, you have to get all the people
 * who tweeted the URL, get all the followers of all those people, unique that set of followers, and then count the
 * unique set. It's an intense computation that can involve thousands of database calls and tens of millions of follower
 * records.
 * <p/>
 * This Storm topology does every piece of that computation in parallel, turning what would be a computation that takes
 * minutes on a single machine into one that takes just a couple seconds.
 * <p/>
 * For the purposes of demonstration, this topology replaces the use of actual DBs with in-memory hashmaps.
 *
 * @see <a href="http://storm.apache.org/documentation/Distributed-RPC.html">Distributed RPC</a>
 * 
 * 
我们在微博、论坛进行转发帖子的时候，是对url进行转发。分析给粉丝（关注我的人），那么每一个人的粉丝（关注者可能有重复的情况）。这个例子是统计一下帖子（url）的转发人数

旧版drpc实现。新版参见storm.demo.drpc_trident.TridentReach.java
实现步骤：
第一，获取当前转发帖子的人
第二，获取当前人的粉丝
第三，进行粉丝去重
第四，统计人数
第五，最后使用drpc远程调用topology返回执行结果。
 */
public class ReachTopology {
	
	  public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {{
		     //帖子  转发者
		    put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
		    put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
		    put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
	  }}; 

	  public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {{
		    //转发人  粉丝
		    put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
		    put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
		    put("tim", Arrays.asList("alex"));
		    put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
		    put("adam", Arrays.asList("david", "carissa"));
		    put("mike", Arrays.asList("john", "bob"));
		    put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
	  }};
      /**
       * 根据url得到转发人  bolt
       */
	  public static class GetTweeters extends BaseBasicBolt {
		    @Override
		    public void execute(Tuple tuple, BasicOutputCollector collector) {
		      Object id = tuple.getValue(0); //request id
		      //System.out.println("==========" + id);
		      String url = tuple.getString(1); //url
		      List<String> tweeters = TWEETERS_DB.get(url); //得到转发人
		      if (tweeters != null) {
		        for (String tweeter : tweeters) {
		          collector.emit(new Values(id, tweeter)); //发送转发人
		        }
		      }
		    }
		
		    @Override
		    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("id", "tweeter"));
		    }
	  }
	  /**
	   * 根据转发人，得到粉丝  bolt
	   * 
	   */
	  public static class GetFollowers extends BaseBasicBolt {
		    @Override
		    public void execute(Tuple tuple, BasicOutputCollector collector) {
		      Object id = tuple.getValue(0);  //request id
		      String tweeter = tuple.getString(1);  //得到转发人
		      List<String> followers = FOLLOWERS_DB.get(tweeter); //得到转发人的粉丝
		      if (followers != null) {
		        for (String follower : followers) {
		          collector.emit(new Values(id, follower));  //发送粉丝
		        }
		      }
		    }
		
		    @Override
		    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("id", "follower"));
		    }
	  }
	  
	  /**
	   * 粉丝去重  bolt
	   * 
PartialUniquer通过继承BaseBatchBolt实现了IBatchBolt接口，batch bolt提供了API用于将一批tuples作为整体来处理。每个请求id会创建一个新的batch bolt实例，同时Storm负责这些实例的清理工作。
当PartialUniquer接收到一个follower元组时执行execute方法，将follower添加到请求id对应的HashSet集合中。
Batch bolt同时提供了finishBatch方法用于当这个task已经处理完所有的元组时调用。PartialUniquer发射一个包含当前task所处理的follower ids子集去重后个数的元组。
	   */
	  public static class PartialUniquer extends BaseBatchBolt {
		    BatchOutputCollector _collector;
		    Object _id;
		    Set<String> _followers = new HashSet<String>();
		
		    @Override
		    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
		      _collector = collector;
		      _id = id;
		    }
		
		    @Override
		    public void execute(Tuple tuple) {
		      _followers.add(tuple.getString(1));  //粉丝加入set集合
		    }
		
		    @Override
		    public void finishBatch() {
		      _collector.emit(new Values(_id, _followers.size()));
		    }
		
		    @Override
		    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("id", "partial-count"));
		    }
	  }

	  /**
	   * 最后一个bolt也要写emit和declarer。因为要返回结果
	   * @author qingjian
	   *
	   */
	  public static class CountAggregator extends BaseBatchBolt {
		    BatchOutputCollector _collector;
		    Object _id;
		    int _count = 0;
		
		    @Override
		    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
		      _collector = collector;
		      _id = id;
		    }
		
		    @Override
		    public void execute(Tuple tuple) {
		      _count += tuple.getInteger(1);
		    }
		
		    @Override
		    public void finishBatch() {
		      _collector.emit(new Values(_id, _count));
		    }
		
		    @Override
		    public void declareOutputFields(OutputFieldsDeclarer declarer) {
		      declarer.declare(new Fields("id", "reach"));
		    }
	  }

	  public static LinearDRPCTopologyBuilder construct() {
		    LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
		    //获取转发过url的人
		    builder.addBolt(new GetTweeters(), 4);
		    //获得上面的人的粉丝
		    builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
		    //对粉丝进行去重
		    builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower")); //这里以request-id和follower进行分组，request-id能够将不同请求区分开来，将一个请求的所有数据进行分一个批次。follower用于并行。好像去掉id也是对的
		    //最后进行统计人数
		    builder.addBolt(new CountAggregator(), 3).fieldsGrouping(new Fields("id")); //汇总一个request-id的结果
		    return builder;
	  }

	  public static void main(String[] args) throws Exception {
	    LinearDRPCTopologyBuilder builder = construct();
	    Config conf = new Config();
	    if (args == null || args.length == 0) {
		      conf.setMaxTaskParallelism(3);
		      LocalDRPC drpc = new LocalDRPC();
		      LocalCluster cluster = new LocalCluster();
		      cluster.submitTopology("reach-drpc", conf, builder.createLocalTopology(drpc));
		
		      String[] urlsToTry = new String[]{ "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com" };
		      for (String url : urlsToTry) {
		        System.out.println("Reach of " + url + ": " + drpc.execute("reach", url));
		      }
		
		      cluster.shutdown();
		      drpc.shutdown();
	    }
	    else {
		      conf.setNumWorkers(6);
		      StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
	    }
	}
}
