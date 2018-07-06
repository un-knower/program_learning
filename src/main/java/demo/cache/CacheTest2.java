package demo.cache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
/**
 * expireAfterWrite和expireAfterAccess
 * @author wguangliang
 *
 */
public class CacheTest2 {
  public static void main(String args[]) throws InterruptedException, ExecutionException {
    Cache cache = CacheBuilder.newBuilder().initialCapacity(200000)
        .maximumSize(200000 *4)
        .concurrencyLevel(40)
        .expireAfterAccess(10, TimeUnit.SECONDS)
   
        .build();
    
    cache.put("a", "1");
    Thread.sleep(6000);
    //System.out.println(cache.get("a")); //1 //get方法如果不存在则报错。这里访问过了，所以过期时间重置
    
    cache.put("b", "2");
    
    Thread.sleep(6000);
    
 
    System.out.println(cache.getIfPresent("a"));  //1
   
    System.out.println(cache.getIfPresent("b"));  //2
    
    
     
  }
}
