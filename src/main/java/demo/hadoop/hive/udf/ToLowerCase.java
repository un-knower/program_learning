package demo.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
/**
 * 
 * UDF分为简单UDF和通用UDF
 * 这是简单UDF，需要(1)继承UDF,(2)重写evaluate方法
 * 
 * 大写转小写
 * @author qingjian
 *
 */
public class ToLowerCase extends UDF{
	public Text evaluate(final Text s) {
		if (s==null) {
			return null;
		}
		return new Text(s.toString().toLowerCase());
	}
}
