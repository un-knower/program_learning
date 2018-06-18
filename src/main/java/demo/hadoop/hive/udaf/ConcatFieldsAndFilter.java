package demo.hadoop.hive.udaf;


import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.parse.HiveParser.fileFormat_return;

public class ConcatFieldsAndFilter extends UDAF{
	
	
	public static class ConcatFieldsAndFilterEvaluator implements UDAFEvaluator {
//		//全局定义输出结果
//		public static class PartialResult{
//			String result;
//			int length;
//		}
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
		public void init() {
			partial = new PartialResult();
			partial.length=0;
			partial.result=new String("");
		}
		
		//iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean
		public boolean iterate(String vid,String time,String playtime, String deli,int filterNum) {
			String value = vid+" "+time+" "+playtime;
			partial.length++;
			partial.filterNum=filterNum;
			if( "".equals(deli)||null==deli) {
				deli=",";
			}
			if(partial.result.length()>0) {
				partial.result=partial.result.concat(deli); //这样第一个前面就不会有分隔符
			}
			partial.delimiter=new String(deli);
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
			if( partial==null) {
				partial=new PartialResult();
				partial.delimiter=other.delimiter;
				partial.filterNum=other.filterNum;
				partial.length=other.length;
				partial.result=other.result;
			}else {
				
				if(partial.result.length()>0) {
					partial.result=partial.result.concat(partial.delimiter); //这样第一个前面就不会有分隔符
				}
				partial.result=partial.result.concat(partial.result);
				partial.length++;
			}
			return true;
		}

		//terminate返回最终的聚集函数结果
		public String terminate() {
			if(partial==null||partial.length<partial.filterNum) {
				return null;
			}else {
				return new String(partial.result);
			}
		}
	}
}
