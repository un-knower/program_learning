package demo.hadoop.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveManage {
	/**
	 * 执行之前，在机器上打开hiveservice,需要打开50000端口:
	 * hive --service hiveserver -p 50000 &
	 * 
	 * Hive0.11.0版本提供了一个全新的服务：HiveServer2
	 * $HIVE_HOME/bin/hive --service hiveserver2 --hiveconf hive.server2.thrift.port=10001
	 * 
	 * 
	 */
	public static void main(String[] args) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
			Connection conn = DriverManager.getConnection("jdbc:hive2://SparkMaster:10001/default","root","fuleying");
			Statement stmt = conn.createStatement();
			String querySQL = "select * from lesson";
			ResultSet res = stmt.executeQuery(querySQL); 
			ResultSetMetaData metaData = res.getMetaData();
			for (int i=1;i<=metaData.getColumnCount();i++) {
				System.out.print(metaData.getColumnName(i)+"\t");
				//打印表结构 meta
//				lesson.no
//				lesson.course
//				lesson.score
			}System.out.println();
			
			while (res.next()) {
				for(int i=1;i<=metaData.getColumnCount();i++) {
					System.out.print(res.getString(i)+"\t");
				}
				System.out.println();
				
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
}
