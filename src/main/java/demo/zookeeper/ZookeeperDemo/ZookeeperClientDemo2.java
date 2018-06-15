package demo.zookeeper.ZookeeperDemo;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperClientDemo2 {
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		String connectString = "192.168.1.160:2181";
		// 创建一个与服务器的连接 需要(服务端的 ip+端口号)(session过期时间)(Watcher监听注册) 
		ZooKeeper zk = new ZooKeeper(connectString, 3000, new Watcher() {
			// 监控所有被触发的事件
			public void process(WatchedEvent event) {
				System.out.println("回调watcher实例：路径"+event.getPath()+"已经触发了" + event.getType() + "事件！"); 
			}
			
		});
		
		
		 // 创建一个目录节点
        /**
         * CreateMode:
         *  PERSISTENT (持续的，相对于EPHEMERAL，不会随着client的断开而消失)
         *  PERSISTENT_SEQUENTIAL（持久的且带顺序的）
         *  EPHEMERAL (短暂的，生命周期依赖于client session)
         *  EPHEMERAL_SEQUENTIAL  (短暂的，带顺序的)
         */
		 //ACL(Access Control)是由（scheme:expression, perms）对构成。
//		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
//		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
//		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		Thread.sleep(2000);
		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		Thread.sleep(2000);
		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		
		//先后创建，有可能数字后缀不同 testRootPath0000000045，testRootPath0000000046，testRootPath0000000047
        //关闭session
        zk.close();
		
		
	}
}
