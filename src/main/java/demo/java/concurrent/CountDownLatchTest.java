package demo.java.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CountDownLatchTest {
  public static void main(String[] args) throws InterruptedException {
    final CountDownLatch begin = new CountDownLatch(1);
    final CountDownLatch end = new CountDownLatch(10);
    
    final ExecutorService exec = Executors.newFixedThreadPool(10); //模拟10个运动员
    
    for(int idx = 0;idx<10;idx++) {
      final int NO = idx +1;
      Runnable run = new Runnable() {
        @Override
        public void run() {
          try {
            begin.await(); //如果begin计数为0，才能往下执行，否则等待
            Thread.sleep((long)(Math.random()*5000));
            System.out.println("No."+NO+" arrive");
          } catch (InterruptedException e) {
            e.printStackTrace();
          } finally {
            end.countDown();
          }
          
        }
      };
      
      exec.submit(run);
      
    }
    
    System.out.println("Game start");
    begin.countDown(); //begin减一后，为零
    end.await(); //如果end计数为0，才能往下执行，否则等待。当end减为0，则说明所有线程都执行完成了，继续往下执行
    System.out.println("Game over");
    
    exec.shutdown();
    
    
  }
}
