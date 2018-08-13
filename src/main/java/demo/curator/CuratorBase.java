package demo.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * zookeeper 操作类
 * Curator是Netflix公司一个开源的zookeeper客户端，
 * 在原生API接口上进行了包装，解决了很多ZooKeeper客户端非常底层的细节开发。
 * 同时内部实现了诸如Session超时重连，Watcher反复注册等功能，实现了Fluent风格的API接口，是使用最广泛的zookeeper客户端之一。
 */
public class CuratorBase {
    private static final String ZK_ADDR = "192.168.1.160:2181";
    private static final int SESSION_OUTTIME = 5000; //ms   default 60 * 1000
    private static final int CONNECTION_OUTTIME = 5000; //ms default 15 * 1000

    CuratorFramework cf = null;

    /**
     * 初始化
     */
    @Before
    public void initial() {
        // 1 重试策略：初试时间为1s 重试10次
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 10);
        // 2 通过工厂创建连接

        cf  = CuratorFrameworkFactory.builder()
                .connectString(ZK_ADDR)
                .retryPolicy(retryPolicy)
                .sessionTimeoutMs(SESSION_OUTTIME)
                .connectionTimeoutMs(CONNECTION_OUTTIME)
                .build();
        // 上句与下句构建一样
        // CuratorFramework cf = CuratorFrameworkFactory.newClient(ZK_ADDR, SESSION_OUTTIME, CONNECTION_OUTTIME, retryPolicy);

        System.out.println("CuratorFramework state : " + cf.getState());  // LATENT

        // 3 开启连接
        cf.start();
        System.out.println("CuratorFramework state : " + cf.getState());  // STARTED
    }


    /**
     * 创建persistent节点
     * 建立节点 指定节点类型（不加withMode默认为持久类型节点）、路径、数据内容
     * creatingParentsIfNeeded如果不存在则会创建父目录，并默认赋予一个值
     */
    @Test
    public void createPersistentNode() throws Exception {
        String s = cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/test/c1");
        System.out.println(s);  //  /test/c1
        String s2 = cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/test/c2", "c2节点内容".getBytes());
        System.out.println(s2); // /test/c2
/*
        [zk: localhost:2181(CONNECTED) 6] get /test/c1
        10.234.129.171
        cZxid = 0x7da
        ctime = Wed Aug 08 23:48:13 EDT 2018
        mZxid = 0x7da
        mtime = Wed Aug 08 23:48:13 EDT 2018
        pZxid = 0x7da
        cversion = 0
        dataVersion = 0
        aclVersion = 0
        ephemeralOwner = 0x0
        dataLength = 14
        numChildren = 0
        [zk: localhost:2181(CONNECTED) 7] get /test/c2
        c2节点内容
        cZxid = 0x7db
        ctime = Wed Aug 08 23:48:13 EDT 2018
        mZxid = 0x7db
        mtime = Wed Aug 08 23:48:13 EDT 2018
        pZxid = 0x7db
        cversion = 0
        dataVersion = 0
        aclVersion = 0
        ephemeralOwner = 0x0
        dataLength = 14
        numChildren = 0
 */

    }

    /**
     * 创建ephemeral节点
     * 建立临时节点 指定节点类型（不加withMode默认为持久类型节点）、路径、数据内容
     */
    @Test
    public void createEphemeralNode() throws Exception {
        String s = cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/test/c3");
        System.out.println(s);  //  /test/c3
        String s2 = cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/test/c4", "c4节点内容".getBytes());
        System.out.println(s2); // /test/c4

        // ephemeral 客户端断开连接则节点消失
        byte[] ret1 = cf.getData().forPath("/test/c3");
        System.out.println(new String(ret1));               // 10.234.129.171

        byte[] ret2 = cf.getData().forPath("/test/c4");
        System.out.println(new String(ret2));               // c2节点内容

    }

    /**
     * 读取节点信息
     * @throws Exception
     */
    @Test
    public void getNode() throws Exception {
        byte[] ret1  = cf.getData().forPath("/test/c1");
        System.out.println(new String(ret1));               // 10.234.129.171
        byte[] ret2  = cf.getData().forPath("/test/c2");
        System.out.println(new String(ret2));               // c2节点内容


    }


    /**
     * 修改节点内容
     * @throws Exception
     */
    @Test
    public void updateNode() throws Exception {
        Stat stat = cf.setData().forPath("/test/c2", "update c2节点内容".getBytes());
        System.out.println(stat.toString());        // 2027,2039,1533795289326,1533795931023,2,0,0,0,21,0,2027


        byte[] ret1  = cf.getData().forPath("/test/c2");
        System.out.println(new String(ret1));               // 10.234.129.171

    }

    /**
     * 异步绑定回调函数
     * 比如创建节点时绑定一个回调函数，该回调函数可以输出服务器的状态码以及服务器事件类型
     * @throws Exception
     */
    @Test
    public void callBack() throws Exception {
        ExecutorService pool = Executors.newCachedThreadPool();
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                   .inBackground(new BackgroundCallback() {
                       @Override
                       public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                           int resultCode = event.getResultCode();
                           System.out.println("code:"+resultCode);  // code:0
                           CuratorEventType type = event.getType(); // type:CREATE
                           System.out.println("type:"+type);
                           System.out.println("线程为:"+Thread.currentThread().getName());  // 线程为:pool-3-thread-1

                       }
                   }, pool).forPath("/test/c5", "c5内容".getBytes());
     }

    /**
     * 判断节点是否存在
     * @throws Exception
     */
    @Test
    public void checkExists() throws Exception {
         // 遍历子目录
         List<String> children = cf.getChildren().forPath("/test");
         for (String child : children) {
             System.out.println(child);
         }

         Stat stat = cf.checkExists().forPath("/test/c5");
         System.out.println(stat == null);   // false

         Stat stat2 = cf.checkExists().forPath("/test/notexist");
         System.out.println(stat2 == null);  // true
    }

    /**
     * 只显示该目录下所有节点 （目录下的目录下的不展示）
     * @throws Exception
     */
    @Test
    public void listPath() throws Exception {
         // 遍历子目录
         List<String> children = cf.getChildren().forPath("/test");
         for (String child : children) {
             System.out.println(child);
         }
    }


    /**
     * 删除节点
     * @throws Exception
     */
    @Test
    public void deleteNode() throws Exception {
        // 需要先判断是否存在，不存在的话报错：
        // org.apache.zookeeper.KeeperException$NoNodeException: KeeperErrorCode = NoNode for /test
        Void aVoid = cf.delete().guaranteed().deletingChildrenIfNeeded().forPath("/test");
    }


    /**
     * 全局计数器
     * 计数器是用来计数的, 利用ZooKeeper可以实现一个集群共享的计数器。
     * 只要使用相同的path就可以得到最新的计数器值， 这是由ZooKeeper的一致性保证的。
     * Curator有两个计数器， 一个是用int来计数，一个用long来计数。
     * @throws Exception
     */
    @Test
    public void getNextAtomicIncrement() throws Exception {
        DistributedAtomicLong atomicLong = new DistributedAtomicLong(cf, "/test/a", new ExponentialBackoffRetry(1000, 3));
        System.out.println("initial pre = "+atomicLong.get().preValue());
        System.out.println("initial post = "+atomicLong.get().postValue());
        AtomicValue<Long> increment = atomicLong.increment();
        if (!increment.succeeded()) {
            System.out.println("getAtomic increment failed!");
        }
        Long preValue = increment.preValue();
        Long postValue = increment.postValue();
        System.out.println("preValue="+preValue);
        System.out.println("postValue="+postValue);

//第一次执行
//        initial pre = 0
//        initial post = 0
//        preValue=0
//        postValue=1

//第二次执行
//        initial pre = 1
//        initial post = 1
//        preValue=1
//        postValue=2

    }

    /**
     * Curator提供了三种类型的缓存方式：Path Cache,Node Cache 和Tree Cache。
     * Path Cache用来监控一个ZNode的子节点，只关心子节点。
     * 当一个子节点增加， 更新，删除时， Path Cache会改变它的状态， 会包含最新的子节点， 子节点的数据和状态。
     * 这也正如它的名字表示的那样， 那监控path。
     *
     * @throws Exception
     */
    @Test
    public void setPathChildrenCacheListener() throws Exception {
        //                                                   CuratorFramework, 监控目录， 是否缓存数据
        PathChildrenCache pathChildrenCache = new PathChildrenCache(cf, "/test", true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                ChildData data = pathChildrenCacheEvent.getData();
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                if (data != null) {
                    if (type.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                        System.out.println("NODE ADDED:"+data.getPath()+", content:"+new String(data.getData())+", time:"+data.getStat().getMtime());

                    } else if (type.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                        System.out.println("NODE REMOVED:"+data.getPath()+", content:"+new String(data.getData())+", time:"+data.getStat().getMtime());
                    } else if (type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                        System.out.println("NODE UPDATE:"+data.getPath()+", content:"+new String(data.getData())+", time:"+data.getStat().getMtime());
                    } else {
                        System.out.println("event.getType=" + type + " is not support");
                    }
                } else {
                    System.out.println("data is null:"+ type);
                }
            }
        });

        // 启动监听
        pathChildrenCache.start();


        Thread.sleep(Integer.MAX_VALUE);
/*
        执行createPersistentNode方法
        NODE ADDED:/test/c1, content:10.234.129.171, time:1533805913931
        NODE ADDED:/test/c2, content:c2节点内容, time:1533805914216


        执行createEphemeralNode方法
        NODE ADDED:/test/c3, content:10.234.129.171, time:1533805943217
        NODE ADDED:/test/c4, content:c4节点内容, time:1533805943219
        NODE REMOVED:/test/c4, content:c4节点内容, time:1533805943219
        NODE REMOVED:/test/c3, content:10.234.129.171, time:1533805943217

        执行updateNode方法
        NODE UPDATE:/test/c2, content:update c2节点内容, time:1533805996796


        执行deleteNode方法
        NODE REMOVED:/test/c1, content:10.234.129.171, time:1533805913931
        NODE REMOVED:/test/c2, content:update c2节点内容, time:1533805996796
*/

    }

    /**
     * Curator提供了三种类型的缓存方式：Path Cache,Node Cache 和Tree Cache。
     * Node Cache用来监控一个ZNode. 当节点的数据修改或者删除时，Node Cache能更新它的状态包含最新的改变。
     * 只关心当前节点
     * @throws Exception
     */
    @Test
    public void setNodeCacheListener() throws Exception {
        NodeCache nodeCache = new NodeCache(cf, "/test");
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                if (nodeCache.getCurrentData() != null) {
                    ChildData data = nodeCache.getCurrentData();
                    System.out.println("NODE CHANGED:"+data.getPath()+", content:"+new String(data.getData()));
                }
            }
        });

        nodeCache.start();


        Thread.sleep(Integer.MAX_VALUE);
/*
        [zk: localhost:2181(CONNECTED) 26] create /test hello
        程序输出：NODE CHANGED:/test, content:hello

        [zk: localhost:2181(CONNECTED) 28] set /test hi
        程序输出：NODE CHANGED:/test, content:hi

        [zk: localhost:2181(CONNECTED) 29] set /test/a he
        [zk: localhost:2181(CONNECTED) 30] create /test/a he
        [zk: localhost:2181(CONNECTED) 31] create /test/b hello
        [zk: localhost:2181(CONNECTED) 32] delete /test
        [zk: localhost:2181(CONNECTED) 33] delete /test/a
        [zk: localhost:2181(CONNECTED) 34] delete /test/b
        [zk: localhost:2181(CONNECTED) 35] delete /test
*/

    }

    /**
     * Curator提供了三种类型的缓存方式：Path Cache,Node Cache 和Tree Cache。
     * TreeCache这种类型的即可以监控节点的状态，还监控节点的子节点的状态， 类似上面两种cache的组合。
     * 这也就是Tree的概念。 它监控整个树中节点的状态。
     * @throws Exception
     */

    @Test
    public void setTreeCacheListener() {
//        TreeCache treeCache = new TreeCache(cf, "/test");


    }











}
