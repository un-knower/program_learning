package demo.hadoop.hbase.filter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * ValueFilter过滤
 * 不需要指定某个列，含指某个值的列的数据都取出来，混在一起  
 * @author qingjian
 *
 */
public class ValueFilterDemo {
	String tableName  = "scores";
	byte[] familyName = "course".getBytes();
	byte[] columnName1 = "art".getBytes();
	byte[] columnName2 = "math".getBytes();
	
	/**
	 * 
	 * @param args
	 */
	public void testValueFilter(Configuration conf) {
		try {
			HTable table = new HTable(conf, tableName);
			/*
			 *查询任何列满足确定值的内容 
			 * 查询包含97的列
			 */
			System.out.println("查询任何列满足确定值的内容 ");
			/**
			 * 这filter的第二个参数是Comparator，剩余demo可以参见RowkeyFilterDemo.java
			 */
			Filter filter = new ValueFilter(CompareOp.EQUAL, new BinaryComparator("97".getBytes()));
			Scan scan1 = new Scan();
			scan1.setFilter(filter);
			ResultScanner scanner1 = table.getScanner(scan1);
			for (Result result : scanner1) {
				for(Cell cell:result.rawCells()) {
				  System.out.println("rowkey="+Bytes.toString(CellUtil.cloneRow(cell))+
		              ", family="+Bytes.toString(CellUtil.cloneFamily(cell))+
		              ", quarlifier="+Bytes.toString(CellUtil.cloneQualifier(cell))+
		              ", value="+Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			

			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void main(String[] args) {
		ValueFilterDemo rowkeyfilterDemo = new ValueFilterDemo();
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
	    //
		config.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
		
		rowkeyfilterDemo.testValueFilter(config);
	}
	
}
