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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * SingleColumnValueFilter过滤
 * 单列值过滤器，将确定一列的满足条件的列 过滤出来
 * @author qingjian
 *
 */
public class SingleColumnValueFilterDemo {
	String tableName  = "scores";
	byte[] familyName = "course".getBytes();
	byte[] columnName1 = "art".getBytes();
	byte[] columnName2 = "math".getBytes();
	byte[] columnName3 = "notexsit".getBytes();
	
	/**
	 * 
	 * @param args
	 */
	public void testSingleColumnValueFilter(Configuration conf) {
		try {
			HTable table = new HTable(conf, tableName);
			/*
			 *查询某一列值满足确定值的内容 
			 *查询数学成绩为87的列
			 */
			System.out.println("查询某一列值满足确定值的内容 ");
			SingleColumnValueFilter filter = new SingleColumnValueFilter(familyName, columnName3, CompareOp.EQUAL, "87".getBytes());
			filter.setFilterIfMissing(true);//默认为false。作为筛选条件的列本身如果不存在，如果为true，则也会被过滤掉，如果为false，则这样的行也会包含在结果集中。
			Scan scan1 = new Scan();
			scan1.setFilter(filter);
			ResultScanner scanner1 = table.getScanner(scan1);
			for(Result result:scanner1) {
		        for(Cell cell : result.rawCells()) {
		          System.out.println("rowkey="+Bytes.toString(CellUtil.cloneRow(cell))+
		              ", family="+Bytes.toString(CellUtil.cloneFamily(cell))+
		              ", quarlifier="+Bytes.toString(CellUtil.cloneQualifier(cell))+
		              ", value="+Bytes.toString(CellUtil.cloneValue(cell)));
		        }
		        
		    }
			//如果filter.setFilterIfMissing(false)则显示如下
//			rowkey=Tom, family=course, quarlifier=art, value=97
//			rowkey=Tom, family=course, quarlifier=math, value=87
//			rowkey=Tom, family=grade, quarlifier=, value=1
//			//如果filter.setFilterIfMissing(true)则没有内容显示
			
			
			/*
			 * 查询某一列值满足某一个范围的内容 
			 * 查询数学成绩为80< <=99之间的列
			 */
			System.out.println("查询某一列值满足某一个范围的内容 ");
			FilterList filterList = new FilterList();
			filterList.addFilter(new SingleColumnValueFilter(familyName, columnName2, CompareOp.GREATER, "80".getBytes()));
			filterList.addFilter(new SingleColumnValueFilter(familyName, columnName2, CompareOp.LESS_OR_EQUAL, "99".getBytes()));
			 
			Scan scan2 = new Scan();
			scan2.setFilter(filterList);
			ResultScanner scanner2 = table.getScanner(scan2);
			for (Result result : scanner2) {
				System.out.println(result);
			}
 
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void main(String[] args) {
		SingleColumnValueFilterDemo rowkeyfilterDemo = new SingleColumnValueFilterDemo();
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
	    //
		config.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
		
		rowkeyfilterDemo.testSingleColumnValueFilter(config);
	}
	
}
