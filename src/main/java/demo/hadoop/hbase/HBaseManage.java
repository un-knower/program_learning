package demo.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

//敦彦静
public class HBaseManage {
	public static final String TABLE_NAME="scores";
	public static final String FAMILY_NAME1="grade";
	public static final String FAMILY_NAME2="course";
	public static final String ROW_KEY="Tom";
	public static void main(String[] args) throws IOException {
		//HBaseConfiguration conf = new HBaseConfiguration(); //不加载HBase的资源hbase-default.xml和hbase-site.xml
		Configuration conf = HBaseConfiguration.create();//加载HBase的资源hbase-default.xml和hbase-site.xml
		//注释掉，仍然运行正常
		conf.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
		//
		conf.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
		
		//创建HBaseAdmin实例，取名为admin
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		//判断scores表是否存在
		if(admin.tableExists(TABLE_NAME)) {
			System.out.println("drop table");
			admin.disableTable(TABLE_NAME);
			admin.deleteTable(TABLE_NAME);
		}
		System.out.println("create table");
		
		
		HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
		
		//创建scores表两个列族grade和course
		tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_NAME1));
		tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_NAME2));
		//创建scores表
		admin.createTable(tableDescriptor);
		
		
		//下面是Shell的添加数据
		System.out.println("put data...");
		HTable hTable = new HTable(conf, TABLE_NAME);//表名称
		Put put = new Put(ROW_KEY.getBytes()); //Tom 为行键
		//         列族 family             列 qualifier   值value
		put.add(FAMILY_NAME1.getBytes(), null, "1".getBytes());
		
		List<Put> putList = new ArrayList<Put>();
		putList.add(put);
		putList.add(new Put(ROW_KEY.getBytes()).add(FAMILY_NAME2.getBytes(), "art".getBytes(), "97".getBytes()));
		putList.add(new Put(ROW_KEY.getBytes()).add(FAMILY_NAME2.getBytes(), "math".getBytes(), "87".getBytes()));
		hTable.put(putList);
		
		//查询数据
		//get查询
		System.out.println("get data");
		Get get = new Get(ROW_KEY.getBytes());
		Result result = hTable.get(get);
		System.out.println("result:"+result);
		byte[] grade = result.getValue(FAMILY_NAME1.getBytes(), null);
		byte[] course_art = result.getValue(FAMILY_NAME2.getBytes(), "art".getBytes());
		byte[] course_math = result.getValue(FAMILY_NAME2.getBytes(), "math".getBytes());
		System.out.println("result:"+result);
		System.out.println("grade:"+new String(result.getValue(FAMILY_NAME1.getBytes(), null))+"\tart:"+new String(course_art)+"\tmath:"+new String(course_math));
		get.addFamily(FAMILY_NAME1.getBytes());
		
	
		
		//scan查询
		System.out.println("scan data...");
		Scan scan = new Scan();
		ResultScanner scanner = hTable.getScanner(scan);
		System.out.println("scan data...根据family和qualify查询");
		for (Result result2 : scanner) {
			byte[] grade2 = result2.getValue(FAMILY_NAME1.getBytes(), null);
			byte[] course_art2 = result2.getValue(FAMILY_NAME2.getBytes(), "art".getBytes());
			byte[] course_math2 = result2.getValue(FAMILY_NAME2.getBytes(), "math".getBytes());
			System.out.println("result:"+result2);
			System.out.println("grade:"+new String(grade2)+"\tart:"+new String(course_art2)+"\tmath:"+new String(course_math2));
				
		}
		scanner = hTable.getScanner(scan);
		System.out.println("scan data...遍历,方法一");
		for(Result r: scanner) {
		  for(KeyValue raw :r.raw()) {
		    System.out.println("rowkey="+new String(raw.getRow())+", family="+new String(raw.getFamily())+",qualify="+new String(raw.getQualifier())+", value="+new String(raw.getValue()));
		  }
		}
		
		System.out.println("scan data...遍历,方法二");
		scanner = hTable.getScanner(scan);
		for(Result r:scanner) {
		  for(Cell cell:r.rawCells()) {
		    System.out.println("kv="+cell+", value="+new String(cell.getValue()));
		    System.out.println("value2 = "+Bytes.toString(CellUtil.cloneValue(cell)));
		    System.out.println("rowkey="+Bytes.toString(CellUtil.cloneRow(cell))+", family="+Bytes.toString(CellUtil.cloneFamily(cell))+", qualify="+Bytes.toString(CellUtil.cloneQualifier(cell))+", value="+Bytes.toString(CellUtil.cloneValue(cell)));
		  }
		}
		
		//关闭
		hTable.close();
		System.out.println("close!");
		
	}
}
