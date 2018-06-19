package demo.hadoop.hbase.syncbatch;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 将经过SyncReadHdfs读取hdfs内容保存的Queue<List<Put>> dataQueue 传递进来
 * 
 * @author qingjian
 *
 */
public class SyncBatchImportHbase implements Runnable{
	private static final Logger LOGGER = LoggerFactory.getLogger(SyncBatchImportHbase.class);
	private Queue<List<Put>> queueCache;
	private static Configuration configuration = new Configuration(); 
	private static HBaseAdmin admin;
	private static HTable table;
	private static String tablename;
	
	static {
		//10.10.22.16
				//hbase的配置
				configuration.set("hbase.zookeeper.quorum","10.10.22.11,10.10.22.15,10.10.22.16");// 使用eclipse时必须添加这个，否则无法定位
				//configuration.set("zookeeper.znode.parent","/dm-hbase-video");//使用eclipse时必须添加这个，否则无法定位。如果测试注释掉
		        configuration.set("ipc.ping.interval", "3000");
				configuration.set("hbase.zookeeper.property.clientPort", "2182");
				configuration.set("hbase.client.write.buffer", "10242880");
				configuration.set("hbase.client.retries.number", "11");
				configuration.set("hbase.client.pause", "20");
				configuration.set("hbase.security.authentication", "simple");
				configuration.set("hbase.ipc.client.tcpnodelay", "true");
				configuration.set("hbase.rootdir", "hdfs://10.10.22.16:8020/hbase");
				//configuration.set("hbase.tmp.dir", "/data/var/hbase");
				configuration.set("hbase.cluster.distributed", "true");
		
	}
	
	public SyncBatchImportHbase(Queue<List<Put>> queueCache,String tablename,String column) throws IOException {
		this.queueCache = queueCache;
		this.tablename = tablename;
		admin = new HBaseAdmin(configuration);
		
		
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
		createTable(tablename,column);
//		if(!admin.tableExists(tablename.getBytes())) {
//			
//			createTable(tablename,column);
//		}
		table = new HTable(configuration,tablename);
		table.setWriteBufferSize(104857600L);
		table.setAutoFlush(false,true);
		//table.setAutoFlushTo(false); !!!!!!!!!
		
	}
	//创建hbase表
	public void createTable(String tablename,String column) throws IOException {
		HTableDescriptor htableDescriptor = new HTableDescriptor(tablename);
		String[] splitColumn = column.split(",");
		for (String stringColumn : splitColumn) {
			htableDescriptor.addFamily(new HColumnDescriptor(KeyValue.parseColumn(Bytes.toBytes(stringColumn))[0]));
		}
		admin.createTable(htableDescriptor);
	}
	
	//向hbase中插入数据
	public void putData(List<Put> putLinkedList) throws IOException {
		table.put(putLinkedList);
		table.flushCommits();
	}
	@Override
	public void run() {
		for(;;) {
			List<Put> listput = null;
			synchronized (queueCache) {
				listput = queueCache.poll();
				while(listput == null) { //如果取出数据为空,一直循环，直到取出数据
					try {
						queueCache.wait();
						listput = queueCache.poll();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				this.queueCache.notifyAll();
			}
			try {
				
				putData(listput); //向hbase中插入数据
				table.flushCommits();
				LOGGER.info("insert into '"+tablename+"' ...");
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
		}
	}

}
