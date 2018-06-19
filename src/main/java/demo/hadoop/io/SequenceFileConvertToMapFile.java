package demo.hadoop.io;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

/**
 * 在MapFile中搜索将相当于在索引和排序过的SequenceFile中搜索。
 * 所以自然就能联想把SequenceFile转换为MapFile
 * 注意：使用MapFile或SequenceFile虽然可以解决HDFS中小文件的存储问题，但也有一定局限性，如：
	1.文件不支持复写操作，不能向已存在的SequenceFile(MapFile)追加存储记录
	2.当write流不关闭的时候，没有办法构造read流。也就是在执行文件写操作的时候，该文件是不可读取的	
 * @author qingjian
 *
 */
public class SequenceFileConvertToMapFile {
	private static final String data[] = {
		"One,tow,buckle my shoe",
		"Three,four,shut the door",
		"Five,sixe,pick up sticks",
		"Seven,eight,lay them straight",
		"Nine,ten,a big fat hen",
	};
	public static class MapFileFixerTest {
		public void testMapFix(String sqFile) throws Exception{
			String uri = sqFile;
			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.get(URI.create(uri),conf);
			Path path = new Path(uri);
			//                     parent child    DATA_FILE_NAME = "data"
			Path mapData = new Path(path, MapFile.DATA_FILE_NAME);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem,mapData,conf);
			Class keyClass = reader.getKeyClass();
			Class valueClass = reader.getValueClass();
			reader.close();
			//long entries = MapFile.fix(fileSystem, mapData, keyClass, valueClass, false, conf);Exception in thread "main" java.lang.Exception: Missing data file in hdfs://localhost:9000/eclipse/mapFile/data, impossible to fix this.
			long entries = MapFile.fix(fileSystem, path, keyClass, valueClass, false, conf);
			System.out.printf("create MapFile from sequenceFile about %d entries!",entries);
		}
	}
	private static final String SEQ_FILE="hdfs://localhost:9000/eclipse/mapFile"; 
	public static void main(String[] args) throws Exception {
		/**
		 * 操作是进入hdfs://localhost:9000/eclipse/mapFile目录下，删除index文件
		 * 执行该程序
		 * 发现index文件出现了，达到了将SequenceFile文件转换为MapFile的目的
		 */
		
		MapFileFixerTest fixer = new MapFileFixerTest();
		fixer.testMapFix(SEQ_FILE);
		System.out.println("Done!");
	}
}
