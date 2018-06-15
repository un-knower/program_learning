package demo.redis.cluster;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
public class TestCluster {
  public static void main(String[] args) throws IOException {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    Set<HostAndPort> nodes = new HashSet<HostAndPort>();
    nodes.add(new HostAndPort("192.168.1.160", 6380));
    nodes.add(new HostAndPort("192.168.1.160", 6381));
    nodes.add(new HostAndPort("192.168.1.160", 6382));
    nodes.add(new HostAndPort("192.168.1.160", 6383));
    nodes.add(new HostAndPort("192.168.1.160", 6384));
    nodes.add(new HostAndPort("192.168.1.160", 6385));
    nodes.add(new HostAndPort("192.168.1.160", 6386));
    nodes.add(new HostAndPort("192.168.1.160", 6387));
    nodes.add(new HostAndPort("192.168.1.160", 6388));
    
    JedisCluster jedisCluster = new JedisCluster(nodes, poolConfig);//JedisCluster默认封装了连接池
    //redis内部会创建连接池，从连接池中获取连接使用，然后再把连接返回给连接池
    String string = jedisCluster.get("a");
    System.out.println(string);
    String hello = jedisCluster.get("hello");
    System.out.println(hello);
    
    jedisCluster.close(); //关闭
    
  }
}
