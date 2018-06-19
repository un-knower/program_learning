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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 与SingleColumnValueFilter的区别在于：
 * 作为筛选条件的 列 不会包含在返回的结果中
 * @author qingjian
 *
 */
public class SkipFilterDemo {
  String tableName = "scores";
  byte[] familyName = "course".getBytes();
  byte[] columnName1 = "art".getBytes();  
  byte[] columnName2 = "math".getBytes();
  HTable table = null;
  public void testSingColumnValueExcludeFilter(Configuration conf) {
    try {
      table = new HTable(conf, tableName);
      /**
       * 查询某一列值不满足确定值的内容
       * 查询除了 （数学成绩为87的列）的列
       */
      System.out.println("查询某一列值不满足确定值的内容");
      Filter filter = new SingleColumnValueExcludeFilter(familyName, columnName2, CompareOp.EQUAL, "87".getBytes());
      Scan scan = new Scan();
      scan.setFilter(filter);
      
      ResultScanner scanner = table.getScanner(scan);
      for(Result result:scanner) {
        for(Cell cell : result.rawCells()) {
          System.out.println("rowkey="+Bytes.toString(CellUtil.cloneRow(cell))+
              ", family="+Bytes.toString(CellUtil.cloneFamily(cell))+
              ", quarlifier="+Bytes.toString(CellUtil.cloneQualifier(cell))+
              ", value="+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        
      }
//      rowkey=Tom, family=course, quarlifier=art, value=97
//      rowkey=Tom, family=grade, quarlifier=, value=1
//      
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if(table != null) {
        try {
          table.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    
    
  }
  public static void main(String[] args) {
    SkipFilterDemo demo = new SkipFilterDemo();
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
    conf.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
    
    demo.testSingColumnValueExcludeFilter(conf);
    
  }
}
