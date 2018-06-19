package demo.hadoop.io.output.test;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Text;
public class SinaMultipleTextOutputFormat extends
		RCFileMultipleOutputFormat<Text, BytesRefArrayWritable> {

//	@Override
//	protected String generateFileNameForKeyValue(Text key, BytesRefArrayWritable value, String name) {
//		String[] arr = key.toString().split("\t");
//		//根据归属平台和频道归属多级目录划分
//		return arr[0]+"/"+arr[1]+"/"+name;
//	}
	@Override
	protected String generateFileNameForKeyValue(Text key, BytesRefArrayWritable value, String name) {
		//根据归属平台和频道归属多级目录划分
		return key.toString()+"/"+name;
	}
}
