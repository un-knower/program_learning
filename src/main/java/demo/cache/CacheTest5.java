package demo.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.cache.RemovalNotification;

public class CacheTest5 {
  
  static Stack removalStack = new Stack<>();
  
  private static class MyRemovalListener implements RemovalListener<String, String> {

    @Override
    public void onRemoval(RemovalNotification<String, String> notification) {
      System.out.println("removal...");
        removalStack.push(notification.getKey()+":"+notification.getValue());
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
    
    while(true) {
      Thread.sleep(1000);
      System.out.println(cache.getIfPresent("a"));
      System.out.println(cache.getIfPresent("b"));  //只有在拿的时候，才会判断是否过期，触发清除监听器。如果注释该行，则不会自动触发监听器
      System.out.println(cache.getIfPresent("c"));  //只有在拿的时候，才会判断是否过期，触发清除监听器。如果注释该行，则不会自动触发监听器
      System.out.println(cache.getIfPresent("d"));  //只有在拿的时候，才会判断是否过期，触发清除监听器。如果注释该行，则不会自动触发监听器
      System.out.println(cache.getIfPresent("e"));  //只有在拿的时候，才会判断是否过期，触发清除监听器。如果注释该行，则不会自动触发监听器
      if(!removalStack.isEmpty()) {
        String pop = (String)removalStack.pop();
        System.out.println(pop);
      }
     
      cache.invalidate("a"); 
    }
 
  }
}
