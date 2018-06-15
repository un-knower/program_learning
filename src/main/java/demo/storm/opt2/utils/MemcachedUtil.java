package demo.storm.opt2.utils;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class MemcachedUtil {
	private static MemCachedClient memClient = null;
	public static MemCachedClient getInstance() {
		if(memClient==null) {
			try {
				//初始化SockIOPool，管理memcached连接池
				SockIOPool pool = SockIOPool.getInstance();
				String[] server = {Constant.MEMCACHE_HOST_SERVER};
				pool.setServers(server);//服务器ip:port
				//配置缓冲池的一些基础信息
				pool.setInitConn(10);
				pool.setMinConn(5);
				pool.setMaxConn(100);
				pool.setMaxIdle(1000*60*60*6);
				//设置线程休眠时间
				pool.setMaintSleep(30);
				//设置关于TCP连接
				pool.setNagle(false); //禁用nagle算法
				pool.setSocketTO(300);
				pool.setSocketConnectTO(0);
				
				pool.setFailover(true);
				pool.setAliveCheck(true);
				//初始化
				pool.initialize();
				
				//建立memcachedclient对象
				memClient = new MemCachedClient();
				return memClient;
			} catch (Exception e) {
			}
					 
		}
		return memClient;
		
		
	}

	//测试main
	public static void main(String[] args) {
		memClient = MemcachedUtil.getInstance();
		memClient.set("a", "gggg");
		System.out.println(memClient.get("a"));
		
	}
	
	
}
