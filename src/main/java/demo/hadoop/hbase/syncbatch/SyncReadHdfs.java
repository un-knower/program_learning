package demo.hadoop.hbase.syncbatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 多线程读取hdfs，将内容以Put的形式保存到Queue<List<Put>> dataQueue
 * @author qingjian
 *
 */
public class SyncReadHdfs implements Runnable{
	private static final Logger LOGGER = LoggerFactory.getLogger(SyncReadHdfs.class);
	private Queue<List<Put>> dataQueue;
	private Thread cacheTread;
	private String filename ;
	private Configuration conf;
	private FileSystem hdfs;
	private List<Path> listPath = new LinkedList<Path>(); //文件路径集合
	private List<byte[]> familys = null; //列族集合
	private List<byte[]> qualifiers = null; //列集合
	private byte[] family = null;
	private byte[] qualifier = null;
	public SyncReadHdfs(Queue<List<Put>> dataQueue, Thread cacheTread,
			String filename,String column) throws IOException, URISyntaxException {
		this.dataQueue = dataQueue;
		this.cacheTread = cacheTread;
		this.filename = filename;
		conf = new Configuration(); //配置连接hdfs的参数
		//conf.set("fs.default.name", "");
		String confPath ="/usr/lib/hadoop/etc/hadoop" ;
		conf.addResource(new Path(confPath, "hdfs-site.xml"));
		conf.addResource(new Path(confPath, "core-site.xml"));
		conf.set("fs.default.name","hdfs://heracles");
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER"); 
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true");
        UserGroupInformation.setConfiguration(conf);
        conf.set("hadoop.security.authentication", "Kerberos");
        try {
			UserGroupInformation.loginUserFromKeytab("video-mvc/10.13.87.11@HERACLES.SOHUNO.COM", "/data/var/push/video-mvc.keytab");
		} catch (IOException e) {
			e.printStackTrace();
		}
		//hdfs FileSystem
		hdfs = FileSystem.get(new URI(filename),conf);
		//整理 列族 和 列
		
		familys = new ArrayList<byte[]>();
		qualifiers = new ArrayList<byte[]>();
		String[] split = column.split(",");
		for(String columnSplit :split) {
			byte[][] parseColumn = KeyValue.parseColumn(Bytes.toBytes(columnSplit));
			family = parseColumn[0];
			qualifier = parseColumn[1];
			familys.add(family);
			qualifiers.add(qualifier);
		}
	}
	public void visitPath(FileSystem hdfs, String path) throws IOException {
		Path inputPath = new Path(path);
		FileStatus[] listStatus = hdfs.listStatus(inputPath);
		if(listStatus == null) {
			throw new IOException("the path is not correct:"+path);
		}
		for (FileStatus fileStatus : listStatus) {
			if(fileStatus.isDir()) {
				//System.out.println(fileStatus.getPath().getName()+" -dir-");
				LOGGER.info(fileStatus.getPath().getName()+" -dir-");
				visitPath(hdfs,fileStatus.getPath().toString());
			}
			else {
				//System.out.println(fileStatus.getPath().getName()+",len: "+fileStatus.getLen());
				listPath.add(fileStatus.getPath());
				LOGGER.info(fileStatus.getPath().getName()+",len: "+fileStatus.getLen());
			}
		}
	}
	@Override
	public void run() {
		//验证 hdfs输入路径,并将找到的文件路径加入到listPath集合中
		try {
			visitPath(hdfs,filename);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//System.out.println("listpath="+listPath);
		BufferedReader reader = null;
		int count = 0;
		int countall = 0;
		for(Path path:listPath) {
			try {
				FSDataInputStream fs = hdfs.open(path);
				reader = new BufferedReader(new InputStreamReader(fs,"utf-8"));
				
				List<Put> listput = new LinkedList<Put>();
				String infos;
				while((infos=reader.readLine())!=null) {
					//System.out.println("content="+infos);
					countall++;
					//System.out.println("num: "+countall);
					if(count==10000) {//10000条Put数据作为一个List保存值dataQueue一次
						int dataQueueSize = this.dataQueue.size(); //List<Put>个数
						if(dataQueueSize == 0) {
							synchronized (dataQueue) {
								dataQueue.offer(listput); //插入数据
								dataQueue.notify();
							}
							
						}
						else {
							if(dataQueueSize>3) {
								synchronized (dataQueue) {
									try {
										this.dataQueue.wait();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
							}
							while(!this.dataQueue.offer(listput)){}
						}
						listput = null;
						listput = new LinkedList<Put>();
						count=0;
					}//if(count==10000) 
					String[] info = infos.split("\t");
					if(info.length>=2) { //主键，值
						count++;
						//一行内容
						Put putrow = new Put(info[0].getBytes()); //主键
						for(int i=0;i<familys.size();i++) {
							if(i+1<info.length) {
								putrow.add(familys.get(i),qualifiers.get(i),info[i+1].getBytes());
							}
						}
						listput.add(putrow);
					}
				}//while
				synchronized (dataQueue) {
					dataQueue.offer(listput);
					dataQueue.notifyAll();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		LOGGER.info("sleep");
		try {
			Thread.sleep(10000L);
			return;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		finally {
			cacheTread.stop();
			LOGGER.info("stop");
			if(reader!=null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
}
