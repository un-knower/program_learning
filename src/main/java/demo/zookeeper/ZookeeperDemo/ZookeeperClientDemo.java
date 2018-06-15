package demo.zookeeper.ZookeeperDemo;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.omg.CORBA.portable.IDLEntity;

public class ZookeeperClientDemo {
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
		zk.create("/testRootPath", "testRootData".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		
		//取出一个子目录节点
		zk.create("/testRootPath/testChildPathOne", "testChildPathOne".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(zk.getChildren("/testRootPath",true));
		//已经触发了None事件！
		//[testChildPathOne]
		
		// 创建另外一个子目录节点
        zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(), Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);          
        System.out.println(zk.getChildren("/testRootPath",true)); //true表示被watch
//      已经触发了NodeChildrenChanged事件！
//      [testChildPathTwo, testChildPathOne]
        
        //取得节点的数值，返回byte[]
        byte[] data = zk.getData("/testRootPath/testChildPathOne", true, null);
        System.out.println("value = "+new String(data));
        
        //修改节点/root/childone下的数据，第三个参数为版本，如果是-1，那会无视被修改的数据版本，直接改掉
        zk.setData("/testRootPath/testChildPathOne", "childonemodify".getBytes(), -1);
        //已经触发了NodeDataChanged事件！
        

        //删除/root/childone这个节点，第二个参数为版本，－1的话直接删除，无视版本
        zk.delete("/testRootPath/testChildPathTwo", -1);
        //已经触发了NodeChildrenChanged事件！
        
        
        String path = zk.create("/testRootPath/testChildPathThree", "testChildDataThree".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("path="+path); //path=/testRootPath/testChildPathThree
        //关闭session
        zk.close();
		
		
	}
}
