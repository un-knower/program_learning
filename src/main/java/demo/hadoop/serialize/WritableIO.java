package demo.hadoop.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;


public class WritableIO {
	public static void main(String[] args) {

		IntWritable writable = new IntWritable();
		writable.set(7);
		// 将writable序列化为byte数组
		byte[] bytes = serialize(writable);
		String byte_str = StringUtils.byteToHexString(bytes);
		System.out.println(byte_str);  //00000007
		
		IntWritable intw2 = new IntWritable(0);
		deserialize(intw2, bytes);
		System.out.println(intw2);  //7
		

	}

	// 序列化：将Writable对象序列化为byte数组
	public static byte[] serialize(Writable writable) {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		try {
			writable.write(dataOut);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dataOut.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return out.toByteArray();
	}

	//反序列化：将byte数组反序列化为WritableUI小
	public static byte[] deserialize(Writable writable,byte[] bytes) {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		try {
			writable.readFields(dataIn);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				dataIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		return bytes;
	}
	
	
	
}
