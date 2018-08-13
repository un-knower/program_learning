package demo.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * zookeeper 全局锁
 */
public class Curator_Lock {
    static String lock_path = "/curator_recipes_lock_path";
    static CuratorFramework client = CuratorFrameworkFactory.builder().connectString("sparkmaster:2181").retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
    public static void main(String args[]) {
        client.start();
        InterProcessMutex lock = new InterProcessMutex(client, lock_path);

        final CountDownLatch down = new CountDownLatch(1);
        for(int i=0;i<30;i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        down.await();
                        lock.acquire();  // 全局锁加锁

                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss|SSS");
                    String orderNo = sdf.format(new Date());
                    System.err.println("生成的订单号：" + orderNo);
                    try {
                        lock.release(); // 全局锁释放
                    } catch (Exception e) {

                    }
                }
            }).start();

        } // for


        down.countDown();

    }
}
