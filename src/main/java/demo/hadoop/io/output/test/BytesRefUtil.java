package demo.hadoop.io.output.test;

import java.io.IOException;

import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;

public class BytesRefUtil {

	public static String BytesRefWritableToString(BytesRefWritable b){
		Text txt=new Text();
		try {
			txt.set(b.getData(), b.getStart(), b.getLength());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return txt.toString();
	}
	public static BytesRefArrayWritable createRcOutValue(String src) {
		String[] datas = src.split("\t", -1);
		BytesRefArrayWritable reduceValue = new BytesRefArrayWritable();
		BytesRefWritable[] arr = new BytesRefWritable[datas.length];
		for (int j = 0; j < arr.length; j++) {
			arr[j] = new BytesRefWritable();
			arr[j].set(datas[j].getBytes(), 0, datas[j].getBytes().length);
			reduceValue.set(j, arr[j]);
		}
		return reduceValue;
	}
	public static BytesRefArrayWritable createRcOutValue(String src, String delimiter) {
		String[] datas = src.split(delimiter, -1);
		System.out.println("------"+datas.length);
		BytesRefArrayWritable reduceValue = new BytesRefArrayWritable();
		BytesRefWritable[] arr = new BytesRefWritable[datas.length];
		for (int j = 0; j < arr.length; j++) {
			arr[j] = new BytesRefWritable();
			arr[j].set(datas[j].getBytes(), 0, datas[j].getBytes().length);
			reduceValue.set(j, arr[j]);
		}
		return reduceValue;
	}

	public static String[] BytesRefArrayWritableToStringArray(BytesRefArrayWritable braw){
		int len = braw.size();
		String[] data = new String[len];
		for (int i = 0; i < braw.size(); i++) {
			data[i] = BytesRefWritableToString(braw.get(i));
		}
		return data;
	}
}
