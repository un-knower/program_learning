package demo.zookeeper.utils;

import java.util.concurrent.CountDownLatch;

/**
 * Zookeeper分布式锁 向外提供接口 实现用户操作 
 * @author qingjian
 *
 */
public abstract class ALockService {
	private CountDownLatch threadSemaphore;
	private String groupPath;
	public ALockService(CountDownLatch threadSemaphore, String groupPath) {
		this.threadSemaphore = threadSemaphore;
		this.groupPath = groupPath;
	}
	
	public CountDownLatch getThreadSemaphore() {
		return threadSemaphore;
	}
	
	/**
	 * @return the groupPath
	 */
	public String getGroupPath() {
		return groupPath;
	}

	public abstract void doService(); //对外实现提供服务
	
}
