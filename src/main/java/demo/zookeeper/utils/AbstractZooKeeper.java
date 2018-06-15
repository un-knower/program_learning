package demo.zookeeper.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class AbstractZooKeeper implements Watcher {

	// 缓存时间
	private static final int SESSION_TIME = 2000;
	public ZooKeeper zooKeeper;
	protected CountDownLatch countDownLatch = new CountDownLatch(1);
	private String watchPath = null;  //监测路径
	// 定义一个 volatile 成员变量，用于保存最新的 RMI 地址（考虑到该变量或许会被其它线程所修改，一旦修改后，该变量的值会影响到所有线程）
    // volatile让变量每次在使用的时候，都从主存中取。而不是从各个线程的“工作内存”。
    // volatile具有syncronized关键字的“可见性”，但是没有syncronized关键字的“并发正确性”，也就是说不保证线程执行的有序性
    // 也就是说，volatile变量对于每次使用，线程都能得到当前volatile变量的最新值。但是volatile变量并不保证并发的正确性。
	private volatile List<String> dataList = new ArrayList<>(); //保存孩子节点的数据
	public ZooKeeper connect(String hosts) throws IOException, InterruptedException {
		return connect(hosts, SESSION_TIME, null);
	}
	/**
	 * 
	 * @param hosts	zookeeper ip地址
	 * @param workpath	zookeeper的监控目录，监控该目录下的子节点情况 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public ZooKeeper connect(String hosts, String watchPath) throws IOException, InterruptedException {
		return connect(hosts, SESSION_TIME, watchPath);
	}
	/**
	 * 
	 * @param hosts	zookeeper ip地址
	 * @param sessionTimeout	连接超时
	 * @param workpath	zookeeper的监控目录，监控该目录下的子节点情况 
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public ZooKeeper connect(String hosts, int sessionTimeout, String watchPath) throws IOException, InterruptedException {
		this.watchPath = watchPath; 
		zooKeeper = new ZooKeeper(hosts, sessionTimeout, this);
		countDownLatch.await();
		if(watchPath != null && zooKeeper != null) {
			watchNode(watchPath); //观测该path下的孩子节点
		}
		return zooKeeper;
	}
	/**
	 * 监控该path目录下的子节点的情况
	 * @param path
	 */
	private void watchNode(final String path)  {
		try {
//			System.out.println("watch path"+path);
			List<String> nodeList = this.zooKeeper.getChildren(path, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					if(event.getType() == Event.EventType.NodeChildrenChanged) { //如果孩子节点发生了变化
						watchNode(path);
					}
				}
			});
			dataList.clear(); //清空原有的数据
			for (String node : nodeList) {
				byte[] data = this.zooKeeper.getData(path+"/"+node, false, null);
				dataList.add(new String(data));
			}
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void process(WatchedEvent event) {
		if (event.getState() == Event.KeeperState.SyncConnected) {//与leader已经同步了数据
			countDownLatch.countDown();  //保证得到最新的数据
		}
	}

	public void close() throws InterruptedException {
		this.zooKeeper.close();
	}

	
	/**
	 * @return the dataList
	 */
	public List<String> getDataList() {
		return dataList;
	}
	
	/**
	 * @param dataList the dataList to set
	 */
	public void setDataList(List<String> dataList) {
		this.dataList = dataList;
	}
	/**
	 * @return the workpath
	 */
	public String getWorkpath() {
		return watchPath;
	}
	
}
