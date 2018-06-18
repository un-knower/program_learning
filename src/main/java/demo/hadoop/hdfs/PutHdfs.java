package demo.hadoop.hdfs;

import java.io.FileInputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
/**
 * 
 * 
 * 用HDFS存储海量的视频数据
 * 将本地文件夹中的所有文件上传到hdfs上
 * 
 * @author qingjian
 *
 */
public class PutHdfs {
	static final String HDFSDIR="hdfs://localhost:9000/eclipse/movie";
	static final String LOCALDIR="/home/qingjian/workspace/HadoopDemo/movie";
	
	public static void main(String[] args) throws Exception{
		//创建Configuration 
		Configuration cn = new Configuration();
		
		//创建FileSystem
		FileSystem sendhdfs = FileSystem.get(URI.create(HDFSDIR),cn,"hadoop"); //hdfs的文件系统
		FileSystem local = FileSystem.getLocal(cn); //本地文件系统
		
		//本地上传文件路径
		//确定需要上传视频本地路径，相当于设置“缓冲区”中文件路径
		Path localdir = new Path(LOCALDIR);
		
		//设置接收视频流路径，创建HDFS上的movie目录来接收视频流数据
		Path hdfsdir = new Path(HDFSDIR);
		
		//创建movie文件夹
		sendhdfs.mkdirs(hdfsdir);
		
		//创建上传输出流
		FSDataOutputStream out = null;
		//创建输入流
		FileInputStream in = null;
		
		FileStatus[] listStatus = local.listStatus(localdir);
		for(FileStatus status:listStatus) {
//			System.out.println("路径："+status.getPath());
//			System.out.println("名称："+status.getPath().getName());
			in = new FileInputStream(LOCALDIR+"/"+status.getPath().getName());
			out = sendhdfs.create(new Path(hdfsdir.toString()+"/"+status.getPath().getName()));
			IOUtils.copyBytes(in, out, cn, true); //copyBytes(in, out, conf.getInt("io.file.buffer.size", 4096),  close);
			System.out.println("上传 "+LOCALDIR+"/"+status.getPath().getName()+" 成功!");
		}
		
	}
}
