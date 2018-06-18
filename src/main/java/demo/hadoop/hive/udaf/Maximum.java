package demo.hadoop.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.IntWritable;

/**
 * 简单UDAF
 * (1)继承UDAF
 * (2)内部类Evaluator实现UDAFEvaluator
 * @author qingjian
 * 
 * 
 * hive> create table userinfo(age int);
 * hive> load data local inpath '/home/qingjian/桌面/userinfo' into table userinfo;
 * hive>select age from userinfo;
20
10
60
56
30
70
88
80
 *
 * hive>select maxvalue(age) from userinfo;

88

 *与hive内置函数max一样的效果
 *hive> select max(age) from userinfo;
 *
 */
public class Maximum extends UDAF{
	public static class MaximumIntUDAFEvaluator implements UDAFEvaluator {
		private IntWritable result;
		@Override
		public void init() {
			result = null;
		}
		public boolean iterate(IntWritable value) {
			if(value==null) { //检验读入参数
				return true;
			}
			if(result == null) {
				result = new IntWritable(value.get());
			}
			else {
				result.set(Math.max(result.get(), value.get()));
			}
			return true;
 		}
		
		public IntWritable terminatePartial() {
			return result;
		}
		public boolean merge(IntWritable other) {
			return iterate(other);
		}
		public IntWritable terminate() {
			return result;
		}
	
	}
}
