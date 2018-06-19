package demo.hadoop.hive.udaf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * 简单UDAF
 * hive> create table studentScore(name string,score int)
    > row format delimited
    > fields terminated by '\t';

 * hive> select * from studentScore;
OK
A	20
B	10
B	60
B	56
A	30
A	70
B	88
A	80
 * 
 * hive>add jar /home/qingjian/桌面/helloudaf.jar;
 * hive>create temporary function helloudaf as 'udaf.HelloUDAF';
 * hive> select helloudaf(name,score) from studentScore;
 * 
{"A":200,"B":214}

 * 
 * 
 * @author qingjian
 *
 */
public class HelloUDAF extends UDAF{
	public static class Evaluator implements UDAFEvaluator {
		//存放不同学生的总分
		private static Map<String,Integer> ret;
		public Evaluator() {
			super();
			init();
		}
		//初始化
		//Hive会调用此方法来初始化一个UDAF evaluator类
		@Override
		public void init() {
			ret = new HashMap<String, Integer>();
		}
		//map阶段，遍历所有记录
		//将一行新的数据载入到聚合buffer中
		public boolean iterate(String strStudent,int nScore) {
			if(ret.containsKey(strStudent)) {
				int nValue = ret.get(strStudent);//得到该key对应的value
				nValue +=nScore;
				ret.put(strStudent, nValue);
			}
			else {
				ret.put(strStudent, nScore);
			}
			return true;
		}
		
		//返回最终聚合结果
		public Map<String,Integer> terminate() {
			return ret;
		}
		
		//combiner阶段，本例不需要
		//以一种可持久化的方法返回当前聚合的内容
		public Map<String,Integer> terminatePartial() {
			return ret;
		}
		
		//reduce阶段
		//将terminatePartial返回的中间部分聚合结果合并到当前聚合中
		public boolean merge(Map<String,Integer> other) {
			for(Map.Entry<String, Integer> e:other.entrySet()) {
				ret.put(e.getKey(), e.getValue());
			}
			return true;
		}
	}
}
