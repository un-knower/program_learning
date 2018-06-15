package demo.zookeeper.distributedlock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import demo.zookeeper.conf.Constance;
import demo.zookeeper.utils.ALockService;
import demo.zookeeper.utils.ZooKeeperOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 直接运行，即可看到效果
 * 通过线程创建Zookeeper节点进行排序
 * 排在队头的节点的线程先开始执行  //对外提供接口实现用户逻辑代码
 * 执行完毕后删除节点
 * 当排在队头之后的节点检测到队头节点被删除了，该节点进行执行，执行完毕后再删除。。
 * 
 * @author qingjian
 *
 */
public class TestLock {
	private static final int THREAD_NUM = 10; // 用于测试的10个线程
	private static final CountDownLatch threadSemaphore = new CountDownLatch(THREAD_NUM);
	private static final Logger LOG = LoggerFactory.getLogger(TestLock.class);
	private static final String GROUP_PATH = "/disLocks/sub"; //分布锁节点所在路径
	public static void main(String[] args) throws IOException, InterruptedException {
		/**
		 * 连接zookeeper集群
		 */
		final ZooKeeperOperator operator = new ZooKeeperOperator();
		operator.connect(Constance.ZK_CONNECTION_STRING);  //启动了一个Zookeeper实例

		for (int i = 0; i < THREAD_NUM; i++) {
			final int threadId = i;
			new Thread() {
				@Override
				public void run() {
					try {
						operator.new LockService().doService(new ALockService(threadSemaphore, GROUP_PATH) {
							@Override
							public void doService() { //对外提供接口实现用户逻辑代码
								LOG.info("用户实现方法 do service.."+threadId);
							}
						});
					} catch (Exception e) {
						LOG.error("第"+threadId+"个线程抛出异常");
						e.printStackTrace();
					}

				}// run

			}.start();

		}//for
		try {
//        	Thread.sleep(60000);
        	threadSemaphore.await();
        	operator.close();
            LOG.info("所有线程运行结束!");
        } catch (Exception e) {
            e.printStackTrace();
        }

	}
}
