package demo.redis.cluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPubSub;

public class RedisClusterTest {
  JedisCluster jedisCluster = null;
  static String prefix = "luffi:lbl";
  static String KEY_SPLIT=":";
  String nameKey = prefix+KEY_SPLIT+"name";
  @Before
  public void before() {
    String serverStr = "192.168.1.160:6380,"
        + "192.168.1.160:6381,192.168.1.160:6382,"
        + "192.168.1.160:6383,192.168.1.160:6384,"
        + "192.168.1.160:6385,192.168.1.160:6386,"
        + "192.168.1.160:6387,192.168.1.160:6388";
    String[] serverArray = serverStr.split(",");
    //System.out.println(serverStr);
    for (String string : serverArray) {
      System.out.println(string);
    }
    
    
    Set<HostAndPort> nodes = new HashSet<HostAndPort>();
    for (String server : serverArray) {
      String[] ipPortPair = server.split(":");
      nodes.add(new HostAndPort(ipPortPair[0], Integer.valueOf(ipPortPair[1])));
    }
    
    //                              node timeout 最大重试次数      连接池 
    jedisCluster = new JedisCluster(nodes, 1000, 1, new GenericObjectPoolConfig());
    
    jedisCluster.del(nameKey);
    
  }
  
  /**
   * 发布
   */
  @Test
  public void publish() {
    //                                       频道号          信息
    System.out.println(jedisCluster.publish("channel1", "ss"));
  }
  
  /**
   * 订阅，注意：需要先订阅，然后发布才可以收到信息
   */
  @Test
  public void subscribe() {
    //jedisCluster.psubscribe(new JedisPubSubListener(), "channel1");//带通配符
    jedisCluster.subscribe(new JedisPubSubListener(), "channel1");
  }
  
  /**
   * 简单字符串读写
   */
  @Test
  public void setAndGetStringData() {
    System.out.println(jedisCluster.set(nameKey, "张三"));   //OK
    System.out.println(jedisCluster.get(nameKey)); //张三
    
    System.out.println(jedisCluster.getSet(nameKey, "张三new")); //张三
    System.out.println(jedisCluster.get(nameKey)); //张三new
  }
  
  /**
   * setnx  如果key存在，返回0。如果不存在，则设置成功，返回1
   * setnx  的意思是set if not exist
   */
  @Test
  public void setnxTest() {
    System.out.println(jedisCluster.setnx(nameKey, "张三"));
    System.out.println(jedisCluster.get(nameKey));
    System.out.println(jedisCluster.setnx(nameKey, "张三2"));
    System.out.println(jedisCluster.get(nameKey));
  }
  
  /**
   * 简单字符串读写，带过期时间
   * @throws InterruptedException
   */
  @Test
  public void setexTest() throws InterruptedException {
    System.out.println(jedisCluster.setex(nameKey, 3, "张三"));
//    jedisCluster.setex(key, seconds, value)
//    jedisCluster.pexpire(key, milliseconds)
//    jedisCluster.expire(key, seconds)
    for(int i=0;i<5;i++) {
      System.out.println(jedisCluster.get(nameKey));
      Thread.sleep(1000);
    }
  }
  
  /**
   * 操作子字符串
   */
  @Test
  public void setrangeTest() {
    System.out.println(jedisCluster.set(nameKey, "123456789@qq.com"));
    System.out.println(jedisCluster.get(nameKey));  //123456789@qq.com
    
    //从offset=8开始 替换 字符串value的值(在原有的基础上进行替换，更新)
    System.out.println(jedisCluster.setrange(nameKey, 8, "abcaaaaaaa"));   //18
    System.out.println(jedisCluster.get(nameKey));   //12345678abcaaaaaaa
    
    
    System.out.println(jedisCluster.setrange(nameKey, 8, "abc"));  //18 
    System.out.println(jedisCluster.get(nameKey));   //12345678abcaaaaaaa
    
    //查询子串，返回startOffset到endOffset的字符
    System.out.println(jedisCluster.getrange(nameKey, 2, 5));  //3456
    
    
    
  }
  /**
   * 批量操作key
   * keySlot算法中，如果key包含{}，就会使用第一个{}内部的字符串作为hash key，这样就可以保证拥有同样{}内部字符串的key就会拥有相同slot。
   * 参考  http://brandnewuser.iteye.com/blog/2314280
   * redis.clients.util.JedisClusterCRC16#getSlot(java.lang.String)
   *
   * 注意：这样的话，本来可以hash到不同的slot中的数据都放到了同一个slot中，所以使用的时候要注意数据不要太多导致一个slot数据量过大，数据分布不均匀！
   *
   * MSET 是一个原子性(atomic)操作，所有给定 key 都会在同一时间内被设置，某些给定 key 被更新而另一些给定 key 没有改变的情况，不可能发生。
   * MSET中的批处理中任意一条执行失败，则整体失败 
   */
    
  @Test
  public void msetTest() throws InterruptedException {
      /**
       * jedisCluster.mset("sf","d","aadf","as");
       * 直接这样写，会报错：redis.clients.jedis.exceptions.JedisClusterException: No way to dispatch this command to Redis Cluster because keys have different slots.
       * 这是因为key不在同一个slot中
       */

      String result = jedisCluster.mset(
          "{" + prefix + KEY_SPLIT + "}" + "name", "张三",  //{luffi:lbl:}
          "{" + prefix + KEY_SPLIT + "}" + "age", "23", 
          "{" + prefix + KEY_SPLIT + "}" + "address", "adfsa", 
          "{" + prefix + KEY_SPLIT + "}" + "score", "100");
      System.out.println(result); //OK

      String name = jedisCluster.get("{" + prefix + KEY_SPLIT + "}" + "name");
      System.out.println(name); //张三

      Long del = jedisCluster.del("{" + prefix + KEY_SPLIT + "}" + "age");
      System.out.println(del); //1

      List<String> values = jedisCluster.mget("{" + prefix + KEY_SPLIT + "}" + "name", "{" + prefix + KEY_SPLIT + "}" + "age", "{" + prefix + KEY_SPLIT + "}" + "address");
      System.out.println(values);  //[张三, null, adfsa]
      
  }
  
  /**
   *  MSETNX 命令：它只会在所有给定 key 都不存在的情况下进行设置操作。
   *  http://doc.redisfans.com/string/mset.html
   */
  @Test
  public void msetnxTest() throws InterruptedException {
      Long msetnx = jedisCluster.msetnx(
              "{" + prefix + KEY_SPLIT + "}" + "name", "msetnx",
              "{" + prefix + KEY_SPLIT + "}" + "age", "237",
              "{" + prefix + KEY_SPLIT + "}" + "address", "adfsa",
              "{" + prefix + KEY_SPLIT + "}" + "score", "100");
      System.out.println(msetnx);

      System.out.println(jedisCluster.mget(
              "{" + prefix + KEY_SPLIT + "}" + "name",
              "{" + prefix + KEY_SPLIT + "}" + "age",
              "{" + prefix + KEY_SPLIT + "}" + "address",
              "{" + prefix + KEY_SPLIT + "}" + "score"));//[张三, 23, adfsa, 100]

      //name这个key已经存在，由于mset是原子的，该条指令将全部失败
      msetnx = jedisCluster.msetnx(
              "{" + prefix + KEY_SPLIT + "}" + "phone", "110",
              "{" + prefix + KEY_SPLIT + "}" + "name", "李四",
              "{" + prefix + KEY_SPLIT + "}" + "abc", "abcabc");
      System.out.println(msetnx);

      //mset是原子性的，所以上句没有执行。
      System.out.println(jedisCluster.mget(
              "{" + prefix + KEY_SPLIT + "}" + "name",
              "{" + prefix + KEY_SPLIT + "}" + "age",
              "{" + prefix + KEY_SPLIT + "}" + "address",
              "{" + prefix + KEY_SPLIT + "}" + "score",
              "{" + prefix + KEY_SPLIT + "}" + "phone",
              "{" + prefix + KEY_SPLIT + "}" + "abc"));//[张三, 23, adfsa, 100, null, null]
  }
  
  
  
  /**
   *  getset:设置key值，并返回旧值
   */
  @Test
  public void getSetTest() {
    System.out.println(jedisCluster.set(nameKey, "zhangsan"));  
    System.out.println(jedisCluster.get(nameKey)); //zhangsan
    System.out.println(jedisCluster.getSet(nameKey, "lisi"));  //zhangsan
    System.out.println(jedisCluster.get(nameKey)); //lisi
    
  }
  
  /**
   * append 追加数据，其返回值是追加数据后的长度
   */
  @Test
  public void appendTest() {
    System.out.println(jedisCluster.append(nameKey, "aa")); //2
    System.out.println(jedisCluster.get(nameKey)); //aa
    System.out.println(jedisCluster.append(nameKey, "bbcc")); //6
    System.out.println(jedisCluster.get(nameKey)); //aabbcc
  }
  
  /**
   *  incrf:
   *  将 key 中储存的数字值增一。

   如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。

   如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。

   本操作的值限制在 64 位(bit)有符号数字表示之内。

   这是一个针对字符串的操作，因为 Redis 没有专用的整数类型，所以 key 内储存的字符串被解释为十进制 64 位有符号整数来执行 INCR 操作。

   返回值：     执行 INCR 命令之后 key 的值。

   这里有问题，最终数据结果大于10000    后续在研究 TODO
   这是因为设置的超时时间太小了，他去重试了，所以最终结果大于10000
   * @throws InterruptedException 
   */
  @Test
  public void incrTest() throws InterruptedException {
    jedisCluster.del("incrNum");
    final AtomicInteger atomicInteger = new AtomicInteger(0);
    final CountDownLatch countDownLatch = new CountDownLatch(10); //10个线程，10=线程数
    ExecutorService executorService = Executors.newFixedThreadPool(10); //线程池里有10个线程
    for(int i=0;i<10;i++) {
      executorService.submit(new Runnable() {
        public void run() {
          //每个线程增加1000次，每个加1
          for(int j=0;j<1000;j++) {
            atomicInteger.incrementAndGet();
            jedisCluster.incr("incrNum");
          }
          countDownLatch.countDown();
        }
      });
    }
    
    countDownLatch.await();
    System.out.println(jedisCluster.get("incrNum"));  //1000
    System.out.println(atomicInteger);  //1000
  }
  
  /**
   * hash 测试
   * @throws InterruptedException 
   */
  @Test
  public void hashTest() throws InterruptedException {
    String hashKey = "hashKey";
    /*hdel */
    jedisCluster.del(hashKey);
    //设置值  key field value
    System.out.println(jedisCluster.hset(hashKey, "field1", "1"));  //1
    //判断 key field是否存在
    System.out.println(jedisCluster.hexists(hashKey, "field1"));    //true
    System.out.println(jedisCluster.hset(hashKey, "field1", "2"));  //0
    
    System.out.println(jedisCluster.hset(hashKey, "field2", "3")); //1
    System.out.println(jedisCluster.hset(hashKey, "field3", "4"));  //1
    
    //查看key的field
    System.out.println(jedisCluster.hkeys(hashKey)); //[field1, field3, field2]
    //查看key的各个field对应的值
    System.out.println(jedisCluster.hgetAll(hashKey));  //{field1=2, field3=4, field2=3}
    //在原有的基础上增加2，要求该value必须是可以转换成数值类型
    System.out.println(jedisCluster.hincrBy(hashKey, "field1", 2)); //4
    
    System.out.println(jedisCluster.hset(hashKey,"field4","abc"));
    try{
    System.out.println(jedisCluster.hincrBy(hashKey, "field4", 2)); //ERR hash value is not an integer
    } catch(Exception e) {
      e.printStackTrace();
    }
    
    System.out.println(jedisCluster.hmget(hashKey, "field1"));  //[4]
    
    System.out.println(jedisCluster.hdel(hashKey, "field3")); //1
    System.out.println(jedisCluster.hgetAll(hashKey));  //{field1=4, field4=abc, field2=3}
    
    System.out.println("hsetnx:"+ jedisCluster.hsetnx(hashKey, "field4", "6"));
    System.out.println(jedisCluster.hmget(hashKey, "field4"));  //[abc]
    System.out.println("hvals"+jedisCluster.hvals(hashKey)); //hvals[4, 3, abc]
    
    System.out.println("expire:"+jedisCluster.expire(hashKey, 3));
    
    for(int i=0; i<5; i++) {
      System.out.println(jedisCluster.hgetAll(hashKey));
      Thread.sleep(1000);
    }

  }
  
  /**
   * 模拟先进先出队列
   * 生产者 消费者
   */
  @Test
  public void queue() {
    String key = prefix + KEY_SPLIT + "queue";
    jedisCluster.del(key);
    System.out.println(jedisCluster.lpush(key, "4","2","3","1"));//4
    System.out.println(jedisCluster.lpush(key, "4")); //5
    System.out.println(jedisCluster.lpush(key, "5")); //6
    System.out.println(jedisCluster.lpush(key, "6")); //7
    
    System.out.println("lrange"+jedisCluster.lrange(key,0 ,1));  //lrange[6, 5] 闭区间
    System.out.println("lindex[2]:"+jedisCluster.lindex(key, 2));  //lindex[2]:4
    //在4前面插入100，只针对第一个4
    System.out.println("linsert:"+jedisCluster.linsert(key, LIST_POSITION.BEFORE, "4", "100")); //8
    //在41前面插入100，如果么有41则不插入
    System.out.println("linsert:"+jedisCluster.linsert(key, LIST_POSITION.BEFORE, "41", "100")); //-1
    //打印全部
    System.out.println("lrange:"+jedisCluster.lrange(key, 0, -1));  //lrange:[6, 5, 100, 4, 1, 3, 2, 4]
    
    //写进去的顺序是12345，读取出来的也是12345
    for(int i=0;i<6;i++) {
      System.out.println(jedisCluster.rpop(key));
    }
    
    
  }
  
  
  /**
   * Set集合
   */
  @Test
  public void setTest() {
    String keyA = "{" + prefix + KEY_SPLIT + "set}a"; //{luffi:lbl:set}a
    String keyB = "{" + prefix + KEY_SPLIT + "set}b";  //{luffi:lbl:set}b
    jedisCluster.del(keyA);
    jedisCluster.del(keyB);
    
    //添加数据
    System.out.println(jedisCluster.sadd(keyA, "a","b","c"));  //3给集合添加数据
    System.out.println(jedisCluster.sadd(keyA, "a"));  //0给集合添加数据，集合是不可以重复的
    System.out.println(jedisCluster.sadd(keyA, "d")); //1
    
    //返回集合
    System.out.println(jedisCluster.smembers(keyA)); //[c, b, d, a]返回集合中的所有数据
    System.out.println(jedisCluster.scard(keyA)); //4
    //判断是否是成员
    System.out.println(jedisCluster.sismember(keyA, "c")); //true
    /*
    从 Redis 2.6 版本开始， SRANDMEMBER 命令接受可选的 count 参数：
如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
     */
    System.out.println(jedisCluster.srandmember(keyA));//d  返回集合中的一个随机元素。 
    System.out.println(jedisCluster.spop(keyA)); //b 移除并返回集合中的一个随机元素。
    System.out.println(jedisCluster.smembers(keyA));//[c, d, a] 返回集合所有数据

    /*
    SMOVE 是原子性操作。
如果 source 集合不存在或不包含指定的 member 元素，则 SMOVE 命令不执行任何操作，仅返回 0 。否则， member 元素从 source 集合中被移除，并添加到 destination 集合中去。
当 destination 集合已经包含 member 元素时， SMOVE 命令只是简单地将 source 集合中的 member 元素删除。
当 source 或 destination 不是集合类型时，返回一个错误。
注：不可以在redis-cluster中使用SMOVE：redis.clients.jedis.exceptions.JedisClusterException: No way to dispatch this command to Redis Cluster because keys have different slots.
解决办法可以参考上面的mset命令，使用“{}”来讲可以设置的同一个slot中
     */
    System.out.println(jedisCluster.smove(keyA, keyB, "a"));//1 返回集合所有数据
    System.out.println("keyA: "+jedisCluster.smembers(keyA));//[b, d] 返回集合所有数据
    System.out.println("keyB: "+jedisCluster.smembers(keyB));//[a] 返回集合所有数据

    System.out.println(jedisCluster.sadd(keyB, "a", "f", "c"));//2 给集合添加数据
    System.out.println(jedisCluster.sdiff(keyA, keyB));//[b, d] 差集 keyA-keyB
    System.out.println(jedisCluster.sinter(keyA, keyB));//[] 交集
    System.out.println(jedisCluster.sunion(keyA, keyB));//[c, b, d, f, a]并集
  }
  
  /**
   * sortedSet 集合 带有分数的Set集合
   * Sorted set是set的一个升级版本，它在set的基础上增加了一个顺序属性，这一属性在添加修改元素时候可以指定，
   * 每次指定后，zset会自动重新按新的值调整顺序，按分数递增排序
   */
  
  @Test
  public void sortedSetTest() {
    String keyA = "{"+prefix + KEY_SPLIT + "sortedSet}a";
    String keyB = "{"+prefix + KEY_SPLIT + "sortedSet}b";
    String keyC = "{"+prefix + KEY_SPLIT + "sortedSet}c";
    
    System.out.println(jedisCluster.zadd(keyA, 10, "aa")); //1
    Map<String, Double> map = new HashMap<String, Double>();
    map.put("b", 8.0);
    map.put("c", 4.0);
    map.put("d", 6.0);
    System.out.println(jedisCluster.zadd(keyA, map));  //3
    
    System.out.println(jedisCluster.zcard(keyA));//4 返回有序集 key 的数量。
    //返回有序集合key中score某个范围的数量
    System.out.println(jedisCluster.zcount(keyA, 3, 8)); //3
    //返回全部成员
    System.out.println(jedisCluster.zrange(keyA, 0, -1));  //[c, d, b, aa] //从小到大 递增
    System.out.println(jedisCluster.zrevrange(keyA, 0, -1)); //[aa, b, d, c]
    
    System.out.println("zrem:"+jedisCluster.zrem(keyA, "c","f")); //zrem:1  //;//移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。
    System.out.println("zrange:"+jedisCluster.zrange(keyA, 0, -1)); //zrange:[d, b, aa]
    
    System.out.println("zremrangeByRank:"+jedisCluster.zremrangeByRank(keyA, 1, 2)); //按下标删除，下标从0开始，闭区间
    System.out.println("zrange:"+jedisCluster.zrange(keyA, 0, -1)); //zrange:[d]
    
    jedisCluster.zadd(keyA, 0.9,"e");
    jedisCluster.zadd(keyA, 1.9,"f");
    jedisCluster.zadd(keyA, 2.0,"g");
    jedisCluster.zadd(keyA, 2.9,"h");
    jedisCluster.zadd(keyA, 3.1,"i");
    System.out.println("zrange:"+jedisCluster.zrange(keyA, 0, -1)); //zrange:[e, f, g, h, i, d]
    System.out.println(jedisCluster.zremrangeByScore(keyA, 1, 3));  //3
    //显示全部数据，递增按序
    System.out.println("zrange:"+jedisCluster.zrange(keyA, 0, -1)); //zrange:[e, i, d, b, aa]
    
    /*
          接下来这几个操作，需要使用"{}"使得key落到同一个slot中才可以
     */
    System.out.println("-------");
    System.out.println(jedisCluster.zadd(keyB, map));
    System.out.println("zrange: "+jedisCluster.zrange(keyB, 0, -1)); //zrange: [c, d, b]

    /*
    ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM|MIN|MAX]
            计算给定的一个或多个有序集的并集，其中给定 key 的数量必须以 numkeys 参数指定，并将该并集(结果集)储存到 destination 。
            默认情况下，结果集中某个成员的 score 值是所有给定集下该成员 score 值之 和 。
    WEIGHTS
            使用 WEIGHTS 选项，你可以为 每个 给定有序集 分别 指定一个乘法因子(multiplication factor)，每个给定有序集的所有成员的 score 值在传递给聚合函数(aggregation function)之前都要先乘以该有序集的因子。
            如果没有指定 WEIGHTS 选项，乘法因子默认设置为 1 。
    AGGREGATE
            使用 AGGREGATE 选项，你可以指定并集的结果集的聚合方式。
            默认使用的参数 SUM ，可以将所有集合中某个成员的 score 值之 和 作为结果集中该成员的 score 值；使用参数 MIN ，可以将所有集合中某个成员的 最小 score 值作为结果集中该成员的 score 值；而参数 MAX 则是将所有集合中某个成员的 最大 score 值作为结果集中该成员的 score 值。
     */
    //合并
    System.out.println("zunionstore:"+jedisCluster.zunionstore(keyC, keyA, keyB));//6//合并keyA和keyB并保存到keyC中
    System.out.println("zrange:"+jedisCluster.zrange(keyC, 0, -1));  //zrange:[e, i, c, d, aa, b]
    
    //交集
    System.out.println("zintersotre:"+jedisCluster.zinterstore(keyC, keyA, keyB));
    System.out.println("zrange:"+jedisCluster.zrange(keyC, 0, -1));  //zrange:[b]
    
  }
  
  
  @Test
  //维护Sorted Set的最大长度
  public void sortedSetTest2() {
    String sortedSetKey = "set";
    jedisCluster.zadd(sortedSetKey, 0.3d, "3");
    Long zadd = jedisCluster.zadd(sortedSetKey, 0.1d, "1");
    jedisCluster.zadd(sortedSetKey, 0.2d, "2");
    jedisCluster.zadd(sortedSetKey, 0.1d, "1");
    jedisCluster.zadd(sortedSetKey, 0.4d, "4");
    jedisCluster.zadd(sortedSetKey, 0.6d, "6");
    jedisCluster.zadd(sortedSetKey, 0.5d, "5");
    
    long demandSetLength = 4; //要求sorted set的最大长度为4
    long currentSetlength = jedisCluster.zcard(sortedSetKey);
    System.out.println("当前zset长度为："+currentSetlength);
    
    if(currentSetlength > demandSetLength) {
      jedisCluster.zremrangeByRank(sortedSetKey, 0,  -(demandSetLength+1));
    }
    
  }
  
  @Test
  //维护Sorted Set中的元素的ttl
  public void sortedSetTest3() {
    String sortedSetKey = "set2";
    long ttl = 30*24*60*60*1000L; //毫秒，30天
    
    jedisCluster.zadd(sortedSetKey, 1519318861000d, "3");  //2018/2/23 1:1:1
    Long zadd = jedisCluster.zadd(sortedSetKey, 1516554061000d, "1");  //2018/1/22 1:1:1
    jedisCluster.zadd(sortedSetKey, 1516640461000d, "2"); //2018/1/23 1:1:1
    jedisCluster.zadd(sortedSetKey, 1519491661000d, "4"); //2018/2/25 1:1:1
    jedisCluster.zadd(sortedSetKey, 1520442061000d, "6"); //2018/3/8 1:1:1
    jedisCluster.zadd(sortedSetKey, 1520096461000d, "5"); //2018/3/4 1:1:1
    
    System.out.println(jedisCluster.zcard(sortedSetKey));
////    long currentTimestamp = System.currentTimeMillis();
    long currentTimestamp = 1520502147218L;
    System.out.println("当前时间戳："+currentTimestamp);
    System.out.println("ttl："+ttl);
    System.out.println("currentTimestamp-ttl："+(currentTimestamp-ttl));  //2018/2/6 17:42:27
    // 会把(当前时间-ttl)之前的数据删除掉
    System.out.println("删除元素个数："+jedisCluster.zremrangeByScore(sortedSetKey, 0d, currentTimestamp-ttl));
       
  }
  @Test
  //维护Sorted Set中的元素的ttl
  public void sortedSetTest4() {
    String sortedSetKey = "set";
    
    jedisCluster.zadd(sortedSetKey, 0.3d, "3");  //2018/2/23 1:1:1
    Long zadd = jedisCluster.zadd(sortedSetKey, 0.1, "1");  //2018/1/22 1:1:1
    jedisCluster.zadd(sortedSetKey, 0.2d, "2"); //2018/1/23 1:1:1
    jedisCluster.zadd(sortedSetKey, 0.4d, "4"); //2018/2/25 1:1:1
    jedisCluster.zadd(sortedSetKey, 0.6d, "6"); //2018/3/8 1:1:1
    jedisCluster.zadd(sortedSetKey, 0.5d, "5"); //2018/3/4 1:1:1
    
    long currentTimestamp = System.currentTimeMillis();
    System.out.println("当前时间戳："+currentTimestamp);
    jedisCluster.zremrangeByScore(sortedSetKey, 0d, 0.4);
    
  }
  @Test
  //维护Sorted Set中的元素的ttl
  public void sortedSetTest5() {
    String sortedSetKey = "set3";
    long ttl = 30*24*60*60*1000; //second
    
    jedisCluster.zadd(sortedSetKey, 3d, "3");  //2018/2/23 1:1:1
    Long zadd = jedisCluster.zadd(sortedSetKey, 0.1, "1");  //2018/1/22 1:1:1
    jedisCluster.zadd(sortedSetKey, 2d, "2"); //2018/1/23 1:1:1
    jedisCluster.zadd(sortedSetKey, 4d, "4"); //2018/2/25 1:1:1
    jedisCluster.zadd(sortedSetKey, 6d, "6"); //2018/3/8 1:1:1
    jedisCluster.zadd(sortedSetKey, 5d, "5"); //2018/3/4 1:1:1
    
    long currentTimestamp = System.currentTimeMillis();
    System.out.println("当前时间戳："+currentTimestamp);
    jedisCluster.zremrangeByScore(sortedSetKey, 0d, 5d);
    
  }
  
  /**
   * 列表 排序
   */
  @Test
  public void sort() {
    String key=prefix + KEY_SPLIT +"queue";
    jedisCluster.del(key);
    
    System.out.println(jedisCluster.lpush(key, "1","5","3","21","6"));
    System.out.println(jedisCluster.lrange(key, 0, -1)); //[6, 21, 3, 5, 1]
    System.out.println(jedisCluster.sort(key)); //[1, 3, 5, 6, 21]
    System.out.println(jedisCluster.lrange(key, 0, -1));  //[6, 21, 3, 5, 1]
    
  }
  
  
  
  
  @After
  public void after(){
      try {
          if(jedisCluster != null) jedisCluster.close();
      } catch (IOException e) {
          e.printStackTrace();
      }
  }
  
  
  
  
  
  
}

class JedisPubSubListener extends JedisPubSub {

  //取得订阅的消息后的处理 
  @Override
  public void onMessage(String channel, String message) {
    System.out.println("channel:"+channel+", message:"+message);
  }
 
  //初始化订阅时候的处理 
  @Override
  public void onSubscribe(String channel, int subscribedChannels) {
    System.out.println("channel:"+channel+", subscribedChannels:"+subscribedChannels);
  }
 
  //取消订阅时候的处理
  @Override
  public void onUnsubscribe(String channel, int subscribedChannels) {
    System.out.println("channel:"+channel+", subscribedChannels:"+subscribedChannels);
  }
 
  //取消按表达式的方式订阅的时候处理
  @Override
  public void onPUnsubscribe(String pattern, int subscribedChannels) {
    System.out.println("pattern:"+pattern+", subscribedChannels:"+subscribedChannels);
  }
  
  //初始化按表达式的方式订阅时候的处理
  @Override
  public void onPSubscribe(String pattern, int subscribedChannels) {
    System.out.println("pattern:"+pattern+", subscribedChannels:"+subscribedChannels);
  }
  
  //取得按表达式的方式订阅的消息后的处理
  @Override
  public void onPMessage(String pattern, String channel, String message) {
    System.out.println("pattern:"+pattern+", channel:"+channel+", message:"+message);
  }
}
