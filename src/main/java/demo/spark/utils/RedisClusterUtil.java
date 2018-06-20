package demo.spark.utils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * <p>
 * redis通用工具类
 * </p>
 *
 */
public class RedisClusterUtil implements Serializable {

    /**
     * 点击redis cluster
     */
    private static JedisCluster clickJedisCluster = null;
    /**
     * 曝光redis cluster
     */
    private static JedisCluster showJedisCluster = null;
    /**
     * 用户画像redis cluster
     */
    private static JedisCluster historyJedisCluster = null;



    /**
     * 创建redis cluster
     * @param serverNodes  serverIp:serverPort,serverIp:serverPort,serverIp:serverPort
     */
    public static JedisCluster getClickJedisCluster(String serverNodes) {
        if (clickJedisCluster == null) {
            synchronized (utils.RedisClusterUtil.class) {
                String[] serverArray = serverNodes.split(",");
                Set<HostAndPort> nodes = new HashSet<HostAndPort>();
                for (String server : serverArray) {
                    String[] ipPortPair = server.split(":");
                    nodes.add(new HostAndPort(ipPortPair[0], Integer.valueOf(ipPortPair[1])));
                }

                JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                jedisPoolConfig.setMaxTotal(100);
                jedisPoolConfig.setMaxIdle(12);
                jedisPoolConfig.setMaxWaitMillis(3000L);
                jedisPoolConfig.setTestOnBorrow(true);
                jedisPoolConfig.setTestOnReturn(true);
                clickJedisCluster = new JedisCluster(nodes, 5000, 6, jedisPoolConfig);
            }

        }
        return clickJedisCluster;
    }
    /**
     * 创建redis cluster
     * @param serverNodes  serverIp:serverPort,serverIp:serverPort,serverIp:serverPort
     */
    public static JedisCluster getShowJedisCluster(String serverNodes) {
        if (showJedisCluster == null) {
            synchronized (utils.RedisClusterUtil.class) {
                String[] serverArray = serverNodes.split(",");
                Set<HostAndPort> nodes = new HashSet<HostAndPort>();
                for (String server : serverArray) {
                    String[] ipPortPair = server.split(":");
                    nodes.add(new HostAndPort(ipPortPair[0], Integer.valueOf(ipPortPair[1])));
                }

                JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                jedisPoolConfig.setMaxTotal(100);
                jedisPoolConfig.setMaxIdle(12);
                jedisPoolConfig.setMaxWaitMillis(3000L);
                jedisPoolConfig.setTestOnBorrow(true);
                jedisPoolConfig.setTestOnReturn(true);
                showJedisCluster = new JedisCluster(nodes, 5000, 6, jedisPoolConfig);
            }

        }
        return showJedisCluster;
    }
    /**
     * 创建redis cluster
     * @param serverNodes  serverIp:serverPort,serverIp:serverPort,serverIp:serverPort
     */
    public static JedisCluster getHistoryJedisCluster(String serverNodes) {
        if (historyJedisCluster == null) {
            synchronized (utils.RedisClusterUtil.class) {
                String[] serverArray = serverNodes.split(",");
                Set<HostAndPort> nodes = new HashSet<HostAndPort>();
                for (String server : serverArray) {
                    String[] ipPortPair = server.split(":");
                    nodes.add(new HostAndPort(ipPortPair[0], Integer.valueOf(ipPortPair[1])));
                }

                JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                jedisPoolConfig.setMaxTotal(100);
                jedisPoolConfig.setMaxIdle(12);
                jedisPoolConfig.setMaxWaitMillis(3000L);
                jedisPoolConfig.setTestOnBorrow(true);
                jedisPoolConfig.setTestOnReturn(true);
                historyJedisCluster = new JedisCluster(nodes, 5000, 6, jedisPoolConfig);
            }

        }
        return historyJedisCluster;
    }



}