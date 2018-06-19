package demo.hadoop.hive.udaf;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MergeByUidVid extends UDAF{
	public static class Evaluator implements UDAFEvaluator {
		//存放不同学生的总分
		private static Map<String,String> ret;
		public Evaluator() {
			super();
			init();
		}
		//初始化
		//Hive会调用此方法来初始化一个UDAF evaluator类
		@Override
		public void init() {
			ret = new HashMap<String, String>();
		}
		//map阶段，遍历所有记录
		//将一行新的数据载入到聚合buffer中
		public boolean iterate(String uid,String vid,String time,int play_time) {
			String key=uid+" "+vid;
			if(ret.containsKey(key)) {
				String nValue = ret.get(key);//得到该key对应的value
				nValue=nValue+" "+time+" "+play_time+",";
				ret.put(key, nValue);
			}
			else {
				ret.put(key, time+" "+play_time+",");
			}
			return true;
		}
		
		//返回最终聚合结果
		public Map<String,String> terminate() {
			return ret;
		}
		
		//combiner阶段，本例不需要
		//以一种可持久化的方法返回当前聚合的内容
		public Map<String,String> terminatePartial() {
			return ret;
		}
		
		//reduce阶段
		//将terminatePartial返回的中间部分聚合结果合并到当前聚合中
		public boolean merge(Map<String,String> other) {
			for(Map.Entry<String, String> e:other.entrySet()) {
				ret.put(e.getKey(), e.getValue());
			}
			return true;
		}
	}
}
