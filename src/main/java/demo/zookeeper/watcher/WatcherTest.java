package com.zookeeper.watcher;

import java.io.IOException;

import demo.zookeeper.utils.ZooKeeperOperator;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;


class ExistsWatcher implements Watcher{

	@Override
	public void process(WatchedEvent event) {
		
		System.out.println("wathcer type: "+event.getType()+" ,state: "+event.getState());
	}
	
}
public class WatcherTest {
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		ZooKeeperOperator operator = new ZooKeeperOperator();
		ZooKeeper zookeeper = operator.connect("master:2181");
		
		ExistsWatcher watcher = new ExistsWatcher();
		
		zookeeper.create("/registry/test", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		zookeeper.create("/registry/test2", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		//对同一个实例watcher和同一个节点，多次注册也只会响应一次。
//		byte[] data = zookeeper.getData("/registry/test", watcher, null);
//		data = zookeeper.getData("/registry/test2", watcher, null);
		//对不同实例和同于个节点，注册几次，响应几次
		byte[] data = zookeeper.getData("/registry/test", new ExistsWatcher(), null);
		data = zookeeper.getData("/registry/test", new ExistsWatcher(), null);
		
		
		System.out.println(new String(data));
		
		Thread.sleep(Long.MAX_VALUE);
	}
}
