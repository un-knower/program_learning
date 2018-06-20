package demo.spark.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * rediscluster sparkstreaming
 */
public class RedisClusterClient implements KryoSerializable {
    public JedisCluster jedisCluster;
    public String serverNodes;

    public RedisClusterClient() {
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
    }

    public RedisClusterClient(String serverNodes) {
        this.serverNodes = serverNodes;
        if (jedisCluster == null) {
            System.out.println("initial RedisClusterClient .."+serverNodes);
            String[] serverArray = serverNodes.split(",");
            Set<HostAndPort> nodes = new HashSet<HostAndPort>();
            for (String server : serverArray) {
                String[] ipPortPair = server.split(":");
                nodes.add(new HostAndPort(ipPortPair[0], Integer.valueOf(ipPortPair[1])));
            }
            Runtime.getRuntime().addShutdownHook(new CleanWorkThread());

            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(300);
            jedisPoolConfig.setMaxIdle(12);
            jedisPoolConfig.setMaxWaitMillis(2000L);
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPoolConfig.setTestOnReturn(true);
            jedisPoolConfig.setTestWhileIdle(true);
            // 防止释放造成空jedis连接
            jedisPoolConfig.setTimeBetweenEvictionRunsMillis(30000);
            jedisPoolConfig.setNumTestsPerEvictionRun(10);
            jedisPoolConfig.setMinEvictableIdleTimeMillis(60000);

            jedisCluster = new JedisCluster(nodes, 2000, 6, jedisPoolConfig);


        }
    }

    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }


    class CleanWorkThread extends Thread {
        @Override
        public void run() {
            System.out.println("Destroy Redis Cluster");
            if (jedisCluster != null) {
                try {
                    jedisCluster.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeObject(output, this.serverNodes);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        serverNodes = kryo.readObject(input, String.class);
        if (jedisCluster == null) {
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
            jedisCluster = new JedisCluster(nodes, 5000, 6, jedisPoolConfig);

        }
     }
}
