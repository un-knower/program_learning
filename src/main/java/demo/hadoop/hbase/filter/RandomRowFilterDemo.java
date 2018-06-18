package demo.hadoop.hbase.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
/**
 * RandomRowFilter过滤
 * 随机取样
 * @author qingjian
 *
 */
public class RandomRowFilterDemo {
	String tableName  = "test_row_filter";
	
	/**
	 * 
	 * @param args
	 */
	public void testRandomRowFilter(Configuration conf) {
		try {
			HTable table = new HTable(conf, tableName);
			/*
			 * 随机取样 
			 * 取出整体的0.5
			 */
			System.out.println("随机取样 0.5");
			Filter filter = new RandomRowFilter(0.5f); 
					
			Scan scan1 = new Scan();
			scan1.setFilter(filter);
			ResultScanner scanner1 = table.getScanner(scan1);
			int i=0;
			for (Result result : scanner1) {
				System.out.println(result);
				i++;
			}
			System.out.println("sum="+i);

			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void main(String[] args) {
		RandomRowFilterDemo rowkeyfilterDemo = new RandomRowFilterDemo();
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
	    //
		config.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
		
		rowkeyfilterDemo.testRandomRowFilter(config);
	}
	
}
