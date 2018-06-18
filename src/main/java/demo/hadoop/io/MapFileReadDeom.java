package demo.hadoop.io;

import java.io.IOException;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
/**
 * MapFile 读取示例
 * @author qingjian
 */

public class MapFileReadDeom {
	private static final String IN_PATH="hdfs://localhost:9000/eclipse/mapFile";
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(IN_PATH), conf);
		MapFile.Reader reader = null; 
		try {
			reader = new MapFile.Reader(fileSystem, IN_PATH, conf);
			//错误，无法读出数据Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while(reader.next(key, value)) {
				System.out.println(key+"\t"+value);
			}
			
		}catch(Exception e) {
			
		}finally {
			System.out.println("close");
			IOUtils.closeStream(reader);
		}
	}
}
