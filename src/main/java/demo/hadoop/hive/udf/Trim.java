package demo.hadoop.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 过滤首尾空格
 * hive> select strip(" a12345aec") from t1 limit 1;
 * a12345aec
 * hive> select strip(" sd22 ",'sadf ') from t1 limit 1;
 * sd22
 * @author qingjian
 *
 */
public class Trim extends UDF{
	private Text res = new Text();
	public Text evaluate(String str) {
		if(str==null) {
			return null;
		}
		res.set(StringUtils.strip(str.toString()));
		return res;
	}
	public Text evaluate(Text str,String stripChars) {
		if(str==null) {
			return null;
		}
		res.set(StringUtils.strip(str.toString()));
		return res;
	}
}
