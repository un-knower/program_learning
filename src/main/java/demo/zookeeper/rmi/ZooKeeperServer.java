package demo.zookeeper.rmi;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;

import demo.zookeeper.conf.Constance;
import demo.zookeeper.java.rmi.ServiceImpl;
import demo.zookeeper.utils.ZooKeeperOperator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;


/**
 * 配置rmi服务高可用性 服务器端
 * 更换port，启动多个ZooKeeperServer
 * @author qingjian
 *
 */
public class ZooKeeperServer {
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		
		/**
		 * 注册rmi服务
		 */
		Remote service = new ServiceImpl(); //rmi server服务
		String ip = "172.21.14.85";
		int port = 11213;
		String url = "rmi://"+ip+":"+port+"/"+service.getClass().getName();
		//本地主机上的远程对象注册表Registry的实例，并指定端口
		LocateRegistry.createRegistry(port);
		//把远程对象注册到RMI注册服务器上
		Naming.rebind(url, service);
		
		
		/**
		 * 连接zookeeper集群
		 */
		ZooKeeperOperator operator = new ZooKeeperOperator();
		operator.connect(Constance.ZK_CONNECTION_STRING);
		/**
		 * 创建znode，将url写入
		 */
		operator.create(Constance.ZK_PROVIDER_PATH, url.getBytes(), CreateMode.EPHEMERAL_SEQUENTIAL);
		
		
		Thread.sleep(Long.MAX_VALUE);
	}
}
