package demo.hadoop.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
/*
 * 
 * 
 * 
 * 
 * 
 * 
 * 为什么有的数据
 * 
 * 
 * 
 */
public class ConcatFieldsAndFilter2 extends UDAF{
	public static class ConcatIDAFEvaluator implements UDAFEvaluator {
		//全局定义输出结果
		public static class PartialResult{
			String result;
			String delimiter;
			int length;
			int filterNum;
		}
		private PartialResult partial;
		
		//init函数类似于构造函数，用于UDAF的初始化
		@Override
		public void init() {  //每一组（group by），初始化一次
			partial = null;
		}
		
		//iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean
		public boolean iterate(String vid,String time,String play_time, String deli,int num) {
			String value=vid+" "+time+" "+play_time;
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
				partial.length=0;
				partial.filterNum=num;
			}
			
			if(partial.result.length()>0) {
				partial.result=partial.result.concat(partial.delimiter);
			}
			partial.length=partial.length+1;
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
			partial.length=partial.length+1;
			return true;
		}

		//terminate返回最终的聚集函数结果
		public String terminate() {
			if(partial.length<partial.filterNum) {
				return null;
			}
			else{
				return new String(partial.length+" "+partial.result);
			}
		}
	}
}
