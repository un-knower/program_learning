package demo.hadoop.hbase.filter;

import java.io.IOException;

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
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
/**
 * 行健过滤
 * @author qingjian
 *
 */
public class RowkeyFilterDemo {
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
				Put put = new Put(String.format("row%03d", i).getBytes()); //row key
				//         列族 family                           列 qualifier                                      值value
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
	public void testRowFilter(Configuration conf) {
		try {
			HTable table = new HTable(conf, tableName);
			
			/**
			 * 比较主键大小   BinaryComparator
			 */
			Scan scan = new Scan();
			System.out.println("小于等于row010的行");
			Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL	, new BinaryComparator("row010".getBytes()));
			scan.setFilter(filter);
			ResultScanner scanner1 = table.getScanner(scan);
			for (Result result : scanner1) {
				System.out.println(result);
			}
			scanner1.close();
			
			
			
			System.out.println("正则获取结尾为5的行");
			Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*5"));
			scan.setFilter(filter2);
			ResultScanner scanner2 = table.getScanner(scan);
			for (Result result : scanner2) {
				System.out.println(result);
			}
			scanner2.close();
			
			
			System.out.println("包含有5的主键");
			Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("5"));
			scan.setFilter(filter3);
			ResultScanner scanner3 = table.getScanner(scan);
			for (Result result : scanner3) {
				System.out.println(result);
			}
			scanner3.close();
			
			System.out.println("开头是row01的");
			Filter filter4 = new RowFilter(CompareOp.EQUAL, new BinaryPrefixComparator("row01".getBytes()));
			scan.setFilter(filter4);
			ResultScanner scanner4 = table.getScanner(scan);
			for (Result result : scanner4) {
				System.out.println(result);
			}
			scanner4.close();
			
			System.out.println("开头是row01的 且 包含5的主键");
			FilterList filterList = new FilterList(); //the fefault operator MUST_PASS_ALL is assumed.
			filterList.addFilter(filter4);
			filterList.addFilter(filter3);
			scan.setFilter(filterList);
			ResultScanner scanner5 = table.getScanner(scan);
			for (Result result : scanner5) {
				System.out.println(result);
			}
			scanner5.close();
			
			
			System.out.println("开头是row01的 或 包含5的主键");
            FilterList filterList2 = new FilterList(FilterList.Operator.MUST_PASS_ONE);
            filterList.addFilter(filter4);
            filterList.addFilter(filter3);
            scan.setFilter(filterList);
            ResultScanner scanner6 = table.getScanner(scan);
            for (Result result : scanner6) {
                System.out.println(result);
            }
            scanner6.close();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void main(String[] args) {
		RowkeyFilterDemo rowkeyfilterDemo = new RowkeyFilterDemo();
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
	    //
		config.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
		
//		rowkeyfilterDemo.init(config); //初始化数据
		rowkeyfilterDemo.testRowFilter(config);
	}
	
}
