package demo.zookeeper.ZookeeperDemo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * zookeeper分布式锁的实现
 *
 * @author qingjian
 */
public class DistributedClient {
    // 超时时间
    private static final int SESSION_TIMEOUT = 5000; //5秒
    // zookeeper server 列表
    private String hosts = "192.168.1.160:2181,192.168.1.161:2181,192.168.1.162:2181";
    private String groupNode = "locks";
    private String subNode = "sub";

    private ZooKeeper zk;
    // 当前client创建的子节点
    private String thisPath;
    // 当前client等待的子节点
    private String waitPath;


    private CountDownLatch latch = new CountDownLatch(1);

    /**
     * 连接zookeeper
     *
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */

    public void connectZookeeper() throws IOException, InterruptedException, KeeperException {
        // 实例化zk
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, new Watcher() {

            public void process(WatchedEvent event) {
                // 连接建立时，打开watcher，唤醒wait在该latch上的线程
                if (event.getState() == KeeperState.SyncConnected) { //The client is in the connected state
                    System.out.println(thisPath + ":" + latch.getCount());
                    latch.countDown();
                    System.out.println(thisPath + ":" + latch.getCount());
                }

                //发生了waitPath的删除事件
                if (event.getType() == EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                    doSomething();
                }


            }
        });

        //等待连接建立,连接建立，latch会较小到0
        latch.await();

        //创建子节点
        this.thisPath = zk.create("/" + groupNode + "/" + subNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        //wait一会，让结果更清晰一些
        Thread.sleep(10);

        //注意，没有必要监听"/locks"的子节点的变化情况,
        List<String> childrenNodes = zk.getChildren("/" + groupNode, false);

        //列表中只有一个子节点，那肯定就是thisPath，说明client获得锁
        if (childrenNodes.size() == 1) {
            doSomething();
        } else {
            String thisNode = thisPath.substring(("/" + groupNode + "/").length());
            //排序

            Collections.sort(childrenNodes);
            int index = childrenNodes.indexOf(thisNode);
            if (index == -1) {
                //never happened
            } else if (index == 0) {
                //index==0，说明thisNode在列表中最小，当前client获得锁
                doSomething();
            } else {
                //获得排名比thisPath前1位的节点
                this.waitPath = "/" + groupNode + "/" + childrenNodes.get(index - 1);

                //在waitingPath上注册监听器，当waiting被删除时，zookeeper会回调监听器的Process方法
                zk.getData(waitPath, true, new Stat());
            }

        }


    }//func

    private void doSomething() {
        System.out.println("gain lock " + thisPath);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("finished " + thisPath);
            //将thisPath删除，监听thisPath的client将获得通知
            //相当于释放锁
            try {
                zk.delete(thisPath, -1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            new Thread() {
                public void run() {

                    DistributedClient dl = new DistributedClient();
                    try {
                        dl.connectZookeeper();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }.start();
        }//for

        System.out.println("sleep...");
        Thread.sleep(Long.MAX_VALUE);

    }


}

//sleep...
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//null:1
//null:0
//gain lock /locks/sub0000000080
//finished /locks/sub0000000080
///locks/sub0000000081:0
///locks/sub0000000081:0
//gain lock /locks/sub0000000081
//finished /locks/sub0000000081
///locks/sub0000000082:0
///locks/sub0000000082:0
//gain lock /locks/sub0000000082
//finished /locks/sub0000000082
///locks/sub0000000083:0
///locks/sub0000000083:0
//gain lock /locks/sub0000000083
//finished /locks/sub0000000083
///locks/sub0000000084:0
///locks/sub0000000084:0
//gain lock /locks/sub0000000084
//finished /locks/sub0000000084
///locks/sub0000000085:0
///locks/sub0000000085:0
//gain lock /locks/sub0000000085
//finished /locks/sub0000000085
///locks/sub0000000086:0
///locks/sub0000000086:0
//gain lock /locks/sub0000000086
//finished /locks/sub0000000086
///locks/sub0000000087:0
///locks/sub0000000087:0
//gain lock /locks/sub0000000087
//finished /locks/sub0000000087
///locks/sub0000000088:0
///locks/sub0000000088:0
//gain lock /locks/sub0000000088
//finished /locks/sub0000000088
///locks/sub0000000089:0
///locks/sub0000000089:0
//gain lock /locks/sub0000000089
//finished /locks/sub0000000089

