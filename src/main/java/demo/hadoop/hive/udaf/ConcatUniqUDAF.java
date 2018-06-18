package demo.hadoop.hive.udaf;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
/*
右侧组查询结果是 去重 之后的
 * 
 * 
与ConcatUDAF.java的区别
ConcatUDAF.java查询结果
Chinese 55,55,87,60,88,77,56,87,100,100,89,99,100,100,100,87
English 82,12,100,20,56,66,35,100,98,98,66,76,100,76,98,52
History 84,58,88,88,88,80,86,88,66,66,68,80,88,66,40,72
Marth   100,25,72,56,100,99,57,71,71,83,74,57,86,59,71


本程序的查询结果：
Chinese 55,88,77,99,56,100,89,60,87
English 66,12,100,56,35,82,52,20,98,76
History 88,66,68,58,80,72,84,40,86
Marth   99,100,56,57,25,59,71,72,83,74,86

 */
public class ConcatUniqUDAF extends UDAF{
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
			String[] split = partial.result.split(partial.delimiter);
			Set<String> uniq = new HashSet<String>();
			for(String value: split) {
				uniq.add(value);
			}
			
			return mkString(uniq);
		}
		
		private String mkString(Set<String> uniq) {
			Iterator<String> iterator = uniq.iterator();
			StringBuffer stringBuffer = new StringBuffer();
			if(iterator.hasNext()) {
				
				stringBuffer.append(iterator.next());
			}
			while(iterator.hasNext()) {
				stringBuffer.append(partial.delimiter).append(iterator.next());
			}
			return stringBuffer.toString();
		}
		
	}
}
