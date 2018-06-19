package demo.hadoop.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * 读取SequenceFile
 * @author qingjian
 *
 */
public class SeqRead {
	private static final String IN_PATH = "hdfs://localhost:9000/eclipse/seqWrite";
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(IN_PATH), conf);
		
		SequenceFile.Reader reader = null;
		try {
			reader = new SequenceFile.Reader(fileSystem, new Path(IN_PATH), conf);
			//Get key/value
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			
			//Read each key/value
			while(reader.next(key, value)) {//从头到尾读取顺序文件，反复调用next()方法迭代读取记录
				
				System.out.println(key+"\t"+value);
			}
		}catch(Exception e) {
			
		}finally {
			System.out.println("close");
			IOUtils.closeStream(reader);
		}
		
	}
}
