package demo.zookeeper.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class ZooKeeperOperator extends AbstractZooKeeper {

	public class LockService {
		private String LOG_PREFIX_OF_THREAD=Thread.currentThread().getName();
		private String waitPath; // 该线程创建的节点的前一个节点的路径
		private String selfPath; // 该线程创建的节点的路径
		private volatile List<String> pathList = new ArrayList<>(); // 保存孩子节点的路径
		private ALockService aLockService = null;
		private CountDownLatch threadSemaphore = null;
		public void doService(ALockService aLockService) throws Exception {
			this.aLockService = aLockService;
			threadSemaphore = aLockService.getThreadSemaphore();
			String groupPath = aLockService.getGroupPath(); //生成节点路径
			selfPath = create(groupPath, null, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println( LOG_PREFIX_OF_THREAD + "节点创建成功, Path:"+selfPath);
			boolean getLock = getLock(getParentPath(groupPath));
			if(getLock) { //该if只有第一个在队头的线程执行进去，只执行一次，其他都不进去该if语句块，其他都是在等待删除操作后，调用process的方法。
				System.out.println("11111111111111"+LOG_PREFIX_OF_THREAD);
				doSomeThing();
				unLock();
				threadSemaphore.countDown();
			}

		}
		private void doSomeThing() {
			System.out.println(LOG_PREFIX_OF_THREAD + "获取锁成功，赶紧干活！");
			aLockService.doService();
			
		}
		/**
		 * 释放锁
		 * 删除该节点
		 */
		private void unLock() {
			try {
				if(isExsit(selfPath) == null) {
					System.out.println(LOG_PREFIX_OF_THREAD+"本节点已经不存在了。。");
					return ;
				} 
				delete(selfPath);
				System.out.println(LOG_PREFIX_OF_THREAD + "删除本节点："+selfPath);
				
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		private boolean getLock(final String path) throws Exception {

			pathList = zooKeeper.getChildren(path, false);
			int index = isMinPath(getChildPath(selfPath), pathList);

			switch (index) {
				case -1: {
					System.out.println(LOG_PREFIX_OF_THREAD + "本节点已经不存在了.." + selfPath);
					return false;
				}
				case 0: {
					System.out.println(LOG_PREFIX_OF_THREAD + "子节点中，我果然是老大" + selfPath);
					return true;
				}
				default: {
					waitPath = path + "/" + pathList.get(index - 1); // 等待前一个节点的路径
					try {
						zooKeeper.getData(waitPath, new Watcher() {
							@Override
							public void process(WatchedEvent event) {
								System.out.println(LOG_PREFIX_OF_THREAD + "收到情报，排我前面的家伙已挂，我是不是可以出山了？");
								try {
									if(getLock(path)) {
										System.out.println("222222222222"+LOG_PREFIX_OF_THREAD);
										doSomeThing();
										unLock();
										threadSemaphore.countDown();
									}
								} catch (Exception e) {
									e.printStackTrace();
								}	

							}
						}, new Stat());
						System.out.println(LOG_PREFIX_OF_THREAD + "获取子节点中，排在我前面的" + waitPath);
						return false;
					} catch (Exception e) {
						if (zooKeeper.exists(waitPath, false) == null) {
							System.out.println(LOG_PREFIX_OF_THREAD + "子节点中，排在我前面的" + waitPath + "已失踪，幸福来得太突然?");
							return getLock(path);
						} else {
							throw e;
						}
					}
				}//default

			}//switch

		}

	}//watchNode

	private static Log log = LogFactory.getLog(ZooKeeperOperator.class.getName());

	/**
	 * Zookeeper操作：创建 创建持久态的znode，不支持多层创建。比如在创建/parent/child的情况下,无/parent.无法通过
	 * 
	 * @param path
	 * @param data
	 * @return 创建节点的路径
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public String create(String path, byte[] data) throws KeeperException, InterruptedException {
		return create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public String create(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
		return create(path, data, Ids.OPEN_ACL_UNSAFE, createMode);
	}

	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode)
			throws KeeperException, InterruptedException {
		return this.zooKeeper.create(path, data, acl, createMode);
	}

	/**
	 * Zookeeper操作：删除节点或者目录
	 * 
	 * @param path
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public void delete(String path) throws InterruptedException, KeeperException {
		Stack<String> childrenAbsolutePath = getChildrenAbsolutePath(path);
		while (!childrenAbsolutePath.isEmpty()) {
			this.zooKeeper.delete(childrenAbsolutePath.pop(), -1); // -1表示无视版本
		}
	}

	/**
	 * Zookeeper操作：判断path是否存在
	 * 
	 * @param path
	 * @return Stat
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stat isExsit(String path) throws KeeperException, InterruptedException {
		if(this.zooKeeper == null) {
			return null;
		}
		return this.zooKeeper.exists(path, false);
	}

	/**
	 * Zookeeper操作：得到当前path下的所有孩子节点 得到指定节点目录下的所有子节点
	 * 
	 * @param path
	 * @return List<String>
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public List<String> getChild(String path) throws KeeperException, InterruptedException {
		List<String> children = this.zooKeeper.getChildren(path, false);
		if (children.isEmpty()) {
			log.debug(path + " 中没有节点");
		} else {
			log.debug(path + " 有节点");
			for (String child : children) {
				log.debug("节点为 " + child);
				System.out.println("节点为 " + child);
			}
		}
		return children;
	}

	/**
	 * 得到该path下的所有孩子节点的路径，包括孩子下的孩子节点
	 *  	     a 
	 *  		/ \ 
	 *          b c 
	 *         / \ 
	 *         d e 
	 * 返回 Stack 
	 * 		abe 
	 * 		abd 
	 * 		ac 
	 * 		ab
	 * @param root
	 * @return Stack<String>
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stack<String> getChildrenAbsolutePath(String root) throws KeeperException, InterruptedException {
		Stack<String> pathStack = new Stack<>();
		pathStack.add(root);
		int topPoint = 0;
		while (topPoint < pathStack.size()) {
			String path = pathStack.get(topPoint);
			topPoint++;
			for (String pathTmp : getChild(path)) {
				pathStack.add(path + "/" + pathTmp);
			}
		}
		return pathStack;
	}

	/**
	 * Zookeeper操作：得到该path节点的数据 获取指定节点的数据
	 * 
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public byte[] getData(String path) throws KeeperException, InterruptedException {
		return this.zooKeeper.getData(path, false, null);
	}

	/**
	 * Zookeeper操作：更新path节点的数据 更新指定节点的数据
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void setData(String path, byte[] data) throws KeeperException, InterruptedException {
		this.zooKeeper.setData(path, data, -1);
	}

	/**
	 * 得到该path节点路径的父路径
	 * /a/b 返回 /a
	 * /c 返回 ""
	 * c 返回 "" 
	 * @param path
	 * @return
	 */
	public String getParentPath(String path) {
		int lastIndexOf = path.lastIndexOf("/");
		if(lastIndexOf == -1) {
			return "";
		}
		return path.substring(0, lastIndexOf);
	}
	/**
	 * 返回该path节点路径的子路径
	 * /a/b/c 返回 c
	 * /c 返回 c
	 * c 返回 c
	 * @param path
	 * @return
	 */
	public static String getChildPath(String path) {
		int lastIndexOf = path.lastIndexOf("/");
		return path.substring(lastIndexOf+1, path.length());
	}

	/**
	 * 从工作路径下的所有数据中随机选择一个
	 * 
	 * @return
	 */
	public String randomLookup() {
		int size = this.getDataList().size();
		if (size > 0) {
			if (size == 1) {
				return this.getDataList().get(0);// 若 urlList 中只有一个元素，则直接获取该元素
			} else {
				return this.getDataList().get(ThreadLocalRandom.current().nextInt(size)); // 若
																							// urlList
																							// 中存在多个元素，则随机获取一个元素
			}
		} else {
			return null;
		}
	}

	private int isMinPath(String path, List<String> pathList) {
		Collections.sort(pathList);
		int index = pathList.indexOf(path);
		return index;
	}

	public static void main(String[] args) {
		ZooKeeperOperator zkoperator = new ZooKeeperOperator();
		try {
			zkoperator.connect("172.21.14.25");
			byte[] data = new byte[] { 'a', 'b', 'c', 'd' };

			String zktest = "Zookeeper的Java API测试";

			zkoperator.create("/root", null);
			// System.out.println(Arrays.toString(zkoperator.getData("/root")));

			zkoperator.create("/root/child1", data);
			// System.out.println(Arrays.toString(zkoperator.getData("/root/child1")));

			zkoperator.create("/root/child2", data);
			// System.out.println(Arrays.toString(zkoperator.getData("/root/child2")));
			zkoperator.create("/root/child2/chi", data);
			// System.out.println(Arrays.toString(zkoperator.getData("/root/child2/chi")));

			zkoperator.create("/root/child", zktest.getBytes());
			// System.out.println("获取设置的信息：" + new
			// String(zkoperator.getData("/root/child")));
			// System.out.println("节点孩子信息");
			// zkoperator.getChild("/root");
			zkoperator.getChildrenAbsolutePath("/root");
			List<String> child = zkoperator.getChild("/root");
			for (String string : child) {
				System.out.println(string);
				//打印不是完整的路径
//				child2
//				child1
//				child
			}
			zkoperator.delete("/root");
			zkoperator.close();
		} catch (IOException | InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}

}
