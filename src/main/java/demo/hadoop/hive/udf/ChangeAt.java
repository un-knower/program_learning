package demo.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 将 %40 转换成@
 * 
 * @author administrator
 *
 */
public class ChangeAt extends UDF{
	public Text evaluate(final Text text) {
		if(text == null) {
			return null;
		}
		String textStr = text.toString();
		return new Text(textStr.replace("%40", "@"));
	}
}
