package demo.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * 数据清洗
 * 循环访问文件路径
 * 循环范文文件内的所有的文件
 * 
 * 字符流读取hdfs文件内容
 * 
 * @author qingjian
 *
 */
public class AttentionPassportFSDataInputStream {
	final static String inpath = "hdfs://sohuvideo-dm-master/tmp/test";
	static List<Path> inputFiles = new ArrayList<Path>();
	private static Path path;
	public static void main(String[] args) throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(inpath), conf);
		if(fileSystem.exists(new Path(inpath))) {
			System.out.println("yes");
		}
		else {
			System.out.println("no");
		}
		FileStatus[] listStatus = fileSystem.listStatus(new Path(inpath));
		for(FileStatus fileStatus:listStatus) {
			System.out.println(fileStatus.getPath());
			if(!fileStatus.isDir()) {
				inputFiles.add(fileStatus.getPath());
			}
		}
		
		for(Path path:inputFiles) {
			FSDataInputStream open = fileSystem.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(open));
			String content ="";
			while((content=br.readLine())!=null) {
				System.out.println(content);
				System.out.println(content.replaceAll("\"", "").replaceAll("Z", "").replaceAll("T", " "));
				
			}
		}
		
	}
	
	

}
