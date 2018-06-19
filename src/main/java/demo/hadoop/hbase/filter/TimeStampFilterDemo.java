package demo.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
/**
 * TimeStamp过滤
 * @author qingjian
 *
 */
public class TimeStampFilterDemo {
	String tableName  = "test_row_filter";
	String familyName1 = "data";
	String familyName2 = "url";
	
	/**
	 * 初始化数据
	 * @param args
	 */
	
	public void init(Configuration conf) {
		//创建表和初始化数据
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if(!admin.tableExists(tableName)) {
				HTableDescriptor htd = new HTableDescriptor(tableName);
				htd.addFamily(new HColumnDescriptor(familyName1));
				htd.addFamily(new HColumnDescriptor(familyName2));
				admin.createTable(htd);
			}
			
			HTable table = new HTable(conf, tableName);
			table.setAutoFlush(false);
			int count = 50;
			for(int i=1;i<=count;i++) {
				Put put = new Put(String.format("row%03d", i).getBytes());
				put.add(familyName1.getBytes(), String.format("col%01d", i%10).getBytes(), String.format("data%03d", i%10).getBytes());
				put.add(familyName2.getBytes(), String.format("col%01d", i%10).getBytes(), String.format("url%03d", i%10).getBytes());
				
				table.put(put);
				
			}
			table.close();
			
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 * @param args
	 */
	public void testTimeStampeFilter(Configuration conf) {
		try {
			HTable table = new HTable(conf, tableName);
			/*
			 *查询确定的时间戳的内容 
			 */
			System.out.println("查询确定的时间戳的内容");
			List<Long> ts = new ArrayList<Long>();
			ts.add(new Long("1470637916930")); //查询确定的timestamp
			ts.add(new Long("1470637652253"));
			ts.add(new Long(15));
			Filter filter = new TimestampsFilter(ts);
			Scan scan1 = new Scan();
			scan1.setFilter(filter);
			ResultScanner scanner1 = table.getScanner(scan1);
			for (Result result : scanner1) {
				System.out.println(result);
			}
			
			/*
			 * 查询时间戳满足给定时间的时间范围的内容
			 * 即在数组ts2中选出满足setTimeRange的时间范围的内容
			 */
			System.out.println("查询时间戳满足给定时间的时间范围的内容");
			List<Long> ts2 = new ArrayList<Long>();
			ts2.add(new Long("1470637430273"));
			ts2.add(new Long("1470637652253"));
			Filter filter2 = new TimestampsFilter(ts2);
			Scan scan2 = new Scan();
			scan2.setFilter(filter2);
			scan2.setTimeRange(new Long("1470637652253"), new Long("1470637916931")); //左闭右开
			ResultScanner scanner2 = table.getScanner(scan2);
			for (Result result : scanner2) {
				System.out.println(result);
			}
			/*
			 * 查询时间戳满足时间范围的内容
			 * 在全量数据中，选出满足setTimeRange的时间范围的内容
			 */
			System.out.println("查询时间戳满足时间范围的内容");
			Scan scan3 = new Scan();
			scan3.setTimeRange(new Long("1470637652253"), new Long("1470637916931")); //左闭右开
			ResultScanner scanner3 = table.getScanner(scan3);
			for (Result result : scanner3) {
				System.out.println(result);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void main(String[] args) {
		TimeStampFilterDemo rowkeyfilterDemo = new TimeStampFilterDemo();
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
	    //
		config.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
		
//		rowkeyfilterDemo.init(config); //初始化数据
		rowkeyfilterDemo.testTimeStampeFilter(config);
	}
	
}
