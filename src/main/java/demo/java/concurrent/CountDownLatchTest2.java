package demo.java.concurrent;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CountDownLatchTest2 {
  public static void main(String[] args) throws InterruptedException {
    final CountDownLatch down = new CountDownLatch(1);


    for(int idx = 0;idx<10;idx++) {
       new Thread(new Runnable() {
         @Override
         public void run() {
           try {
             down.await();
           } catch (InterruptedException e) {
             e.printStackTrace();
           }

           // 下面代码是并行的。。
           SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss|SSS");
           String orderNo = sdf.format(new Date());
           System.err.println("生成的订单号：" + orderNo);
         }
       }).start();
    } // for

    down.countDown();

  }
}

//        生成的订单号：20:12:38|758
//        生成的订单号：20:12:38|782
//        生成的订单号：20:12:38|784
//        生成的订单号：20:12:38|787      重复
//        生成的订单号：20:12:38|787      重复
//        生成的订单号：20:12:38|788      重复
//        生成的订单号：20:12:38|788      重复
//        生成的订单号：20:12:38|790
//        生成的订单号：20:12:38|791      重复
//        生成的订单号：20:12:38|791      重复