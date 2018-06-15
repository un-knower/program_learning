package demo.cache;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.cache.RemovalNotification;

public class CacheTest4_2 {
  
  static Queue<String> removalQueue = new LinkedList<>();
  static Queue<String> waitingQueue = new LinkedList<>();
   
  private static class MyRemovalListener implements RemovalListener<String, String> {

    @Override
    public void onRemoval(RemovalNotification<String, String> notification) {
      System.out.println("removal...");
    
       removalQueue.add(notification.getKey()+":"+notification.getValue()+":"+notification.getCause().name());
    }
    
  }
  
  public static void main(String args[]) throws InterruptedException, ExecutionException {
    RemovalListener<String, String> async = RemovalListeners.asynchronous(new MyRemovalListener(), Executors.newSingleThreadExecutor());
    
    Cache cache = CacheBuilder.newBuilder().initialCapacity(200000)
        .maximumSize(200000 *4)
        .concurrencyLevel(40)
        .expireAfterWrite(10, TimeUnit.SECONDS)
        .removalListener(async) //加入清除监听器
        
        .build();
    
    cache.put("a", "1");
    cache.put("b", "2");
    cache.put("c", "2");
    cache.put("d", "2");
    cache.put("e", "2");
    
    
    ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
            cache.cleanUp();
        }
    }, 0, 3, TimeUnit.SECONDS);
    
    
    
    while(true) {
      Thread.sleep(1000);
       
      // 输出
      if(!removalQueue.isEmpty()) {
        String pop = (String)removalQueue.poll();
        System.out.println(pop);
      }
     
    }// while
 
  }
}
