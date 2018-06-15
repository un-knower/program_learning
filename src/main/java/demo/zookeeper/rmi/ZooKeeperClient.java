package demo.zookeeper.rmi;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;

import demo.zookeeper.conf.Constance;
import demo.zookeeper.java.rmi.IService;
import demo.zookeeper.utils.ZooKeeperOperator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;



/**
 * 配置rmi服务高可用性 客户端
 * 
 * @author qingjian
 *
 */
public class ZooKeeperClient {
	private static volatile List<String> urlList = new ArrayList<>();  //最新的url列表
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException, NotBoundException {
		
		/**
		 * 连接zookeeper集群
		 */
		ZooKeeperOperator operator = new ZooKeeperOperator();
		operator.connect(Constance.ZK_CONNECTION_STRING, Constance.ZK_REGISTRY_PATH);
		
		
		String dataUrl = null;
		IService helloService = null;
		String result = null;
		while(true) {
			dataUrl = operator.randomLookup(); //随机取出来一个url
			System.out.println(dataUrl);
			helloService = (IService) Naming.lookup(dataUrl);
			result = helloService.sayHello("qingjian");
			System.out.println(result);
			Thread.sleep(5000);
		}
		
	}
	
}
