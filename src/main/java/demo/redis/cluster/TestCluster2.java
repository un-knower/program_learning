package demo.redis.cluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TestCluster2 {
  public static void main(String[] args) throws IOException {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    Set<HostAndPort> nodes = new HashSet<HostAndPort>();
    nodes.add(new HostAndPort("10.200.128.178", 6219));
    nodes.add(new HostAndPort("10.200.128.178", 6231));
    nodes.add(new HostAndPort("10.200.128.179", 6240));

    
    JedisCluster jedisCluster = new JedisCluster(nodes, poolConfig);//JedisCluster默认封装了连接池
    //redis内部会创建连接池，从连接池中获取连接使用，然后再把连接返回给连接池
    String string = jedisCluster.get("{50FEE3C4-5DB3-406F-AB25-3746E627CB2F}_2_demo");
    System.out.println(string);

    
    jedisCluster.close(); //关闭
    
  }
}
