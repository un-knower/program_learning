package demo.hadoop.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
/**
 * 生成一个MapFile文件夹，里面由data和index文件
 * data和index两个文件都是SequenceFile格式
 * index包含一部分键和data文件中键到该键偏移量的映射
 * MapFile是排序后的SequenceFile，MapFile的检索效率是高效的，缺点是会消耗一部分内存来存储index数据。
 * 
 * 注意：MapFile并不会把所有的Record都记录到index中，默认情况下，每隔128条记录存储一个索引映射。
 * 当然，记录间隔可人为修改，通过MapFile.Writer的setInterval()方法，或修改io.map.index.interval属性
 * 
 * 
 * 注意，与SequenceFile不同的是，MapFile的KeyClass一定要实现WritableComparable接口，即Key值是可比较的
 * 使用MapFile或SequenceFile虽然可以解决HDFS中小文件的存储问题，但也有一定局限性，例如
 * 	(1)文件不支持复写操作，不能向已存在的SequenceFile（或MapFile）追加存储记录
 *  (2)当Write流不关闭的时候，没有办法构造read流，也就是在执行文件写操作的时候，该文件是不可读取的。
 * @author qingjian
 *
 */
public class MapFileWriteDemo {
	
	private static final String[] DATA= {
		"One,tow,buckle my shoe",
		"Three,four,shut the door",
		"Five,sixe,pick up sticks",
		"Seven,eight,lay them straight",
		"Nine,ten,a big fat hen",
	};
	private static final String OUT_PATH = "hdfs://localhost:9000/eclipse/mapFile"; //最终会生成一个文件夹，文件夹里有data index文件
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(OUT_PATH), conf);
		//key
		IntWritable key = new IntWritable();
		//value
		Text value = new Text();
		//
		MapFile.Writer writer = null;
		try{
			writer = new MapFile.Writer(conf, fileSystem, OUT_PATH, key.getClass(), value.getClass());
			for(int i=0;i<1024;i++) {
				key.set(i+1);
				value.set(DATA[i%DATA.length]);
				writer.append(key, value);
			}
		}catch(Exception e) {
			
		}finally {
			System.out.println("data put success!");
			IOUtils.closeStream(writer);
		}
		
	}
}
