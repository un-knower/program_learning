package demo.hadoop.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
/**
 * 简单UDAF
 * (1)继承UDAF
 * (2)内部静态类Evaluator实现UDAFEvaluator
 * 		重写init(),iterate(输入值类型1,输入值类型2,..),terminatePartial(),merge(输出结果类型 other),terminate()
 * 		其中init,iterate,merge返回boolean，terminatePartial和terminate返回结果变量类型
 * 
 *  1）init函数类似于构造函数，用于UDAF的初始化

    2）iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean

    3）terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据，iterate和terminatePartial类似于hadoop的Combiner

    4）merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean

    5）terminate返回最终的聚合函数结果
 *   
 * @author qingjian
 *
 *
 * * hive> create table studentScore(name string,score int)
    > row format delimited
    > fields terminated by '\t';

 * hive> select * from studentScore;
 *
A	20
B	10
B	60
B	56
A	30
A	70
B	88
A	80
 *
 *hive> select name from studentScore group by name;  
 *A
 *B
 *
 *hive> select name,concat(score,',') from studentScore group by name;
A	20,30,70,80
B	10,60,56,88
 *
 *hive> select name,concat(score,',') from studentScore;
 FAILED: Error in semantic analysis: Line 1:7 Expression not in GROUP BY key 'name'
 */
public class SimpleUDAF extends UDAF{
	public static class ConcatIDAFEvaluator implements UDAFEvaluator {
		//全局定义输出结果
		public static class PartialResult{
			String result;
			String delimiter;
		}
		private PartialResult partial;
		
		//init函数类似于构造函数，用于UDAF的初始化
		@Override
		public void init() {
			partial = null;
		}
		
		//iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean
		public boolean iterate(String value, String deli) {
			if(value==null) {
				 return true;
			}
			//结果变量初始化
			if(partial==null) {
				partial = new PartialResult();
				partial.result = new String("");
				
				if(deli==null||deli.equals("")) {
					partial.delimiter = new String(",");
				}
				else {
					partial.delimiter = new String(deli);
				}
			}
			if(partial.result.length()>0) {
				partial.result=partial.result.concat(partial.delimiter);
			}
			partial.result=partial.result.concat(value);
			return true;
		}
		//terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据
		public PartialResult terminatePartial() {
			return partial;
		}
		//与iterate函数内容类似，但不一样
		//merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean
		public boolean merge(PartialResult other) {
			if (other == null) {
				return true;
			}
			if (partial == null) {
				partial = new PartialResult();
				partial.result = new String(other.result);
				partial.delimiter = new String(other.delimiter);
			} else {
				if (partial.result.length() > 0) {
					partial.result = partial.result.concat(partial.delimiter);
				}
				partial.result = partial.result.concat(other.result);
			}
			return true;
		}

		//terminate返回最终的聚集函数结果
		public String terminate() {
			return new String(partial.result);
		}
	}
}
