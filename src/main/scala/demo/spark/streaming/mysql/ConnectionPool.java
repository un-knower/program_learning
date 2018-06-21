package demo.spark.streaming.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

public class ConnectionPool {
	private static LinkedList<Connection> connectionQueue;
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized static Connection getConnection() throws SQLException {
		if(connectionQueue == null) {
			connectionQueue = new LinkedList<>();
			for(int i=0;i<5;i++) {
				Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
				connectionQueue.push(connection);
			}
		}
		
		
		return connectionQueue.poll();
		
	}                  
	public static void returnConnection(Connection conn) {
		connectionQueue.push(conn);
	}
	
}
