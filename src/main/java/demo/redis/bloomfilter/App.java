package demo.redis.bloomfilter;

import com.github.wxisme.bloomfilter.bitset.RedisBitSet;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import com.github.wxisme.bloomfilter.common.BloomFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * 唯一需要注意的是redis的bitmap只支持2^32大小，对应到内存也就是512MB,数组的下标最大只能是2^32-1。
 * 不过这个限制我们可以通过构建多个redis的bitmap通过hash取模的方式分散一下即可。
 */
public class App {
    private static final int NUM_OF_BLOOM_FILTER = 2;
    public static void main(String args[]) throws InterruptedException {
        //Jedis jedis , String name

        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(false);
        JedisPool pool = new JedisPool(config, "192.168.1.160", 6379, 100000);
        Jedis jedis = pool.getResource();
        RedisBitSet redisBitSet0 = new RedisBitSet(jedis, "test");   // bitmap key为test，一个大约占用redis 256MB
        RedisBitSet redisBitSet1 = new RedisBitSet(jedis, "test2");  // bitmap key为test


        double falsePositiveProbability = 0.000001d;
        int expectedNumberOElements = Integer.MAX_VALUE;  // 预存放元素个数
        BloomFilter bloomFilter0 = new BloomFilter(falsePositiveProbability, expectedNumberOElements);
        bloomFilter0.bind(redisBitSet0);
        BloomFilter bloomFilter1 = new BloomFilter(falsePositiveProbability, expectedNumberOElements);
        bloomFilter1.bind(redisBitSet1);


        List<String> deviceIds = new ArrayList<>();
        deviceIds.add("dev1");
        deviceIds.add("dev2");
        deviceIds.add("dev3");
        deviceIds.add("dev4");
        deviceIds.add("dev5");
        deviceIds.add("dev1");
        deviceIds.add("dev2");
        deviceIds.add("dev3");
        deviceIds.add("dev4");
        deviceIds.add("dev5");
        deviceIds.add("dev6");
        deviceIds.add("dev7");

        for (String devId : deviceIds) {
            switch ( Math.abs(devId.hashCode()) % NUM_OF_BLOOM_FILTER) {  // 一定要加 Math.abs( xx.hashCode() ) 因为xx.hashCode()有可能为负数
                case 0 : {
                    boolean contains = bloomFilter0.contains(devId);
                    System.out.println("contain "+devId+" ? "+contains);
                    if (!contains) {
                        System.out.println("add "+devId);
                        bloomFilter0.add(devId);
                    }
                    bloomFilter0.count();
                    break;
                }
                case 1 : {
                    boolean contains = bloomFilter1.contains(devId);
                    System.out.println("contain "+devId+" ? "+contains);
                    if (!contains) {
                        System.out.println("add "+devId);
                        bloomFilter1.add(devId);
                    }
                    break;
                }
                default: {
                    boolean contains = bloomFilter0.contains(devId);
                    System.out.println("contain "+devId+" ? "+contains);
                    if (!contains) {
                        System.out.println("add "+devId);
                        bloomFilter0.add(devId);
                    }
                    bloomFilter0.add(devId);
                }
            }


        }

    }

}


/*
        contain dev1 ? false
        add dev1
        contain dev2 ? false
        add dev2
        contain dev3 ? false
        add dev3
        contain dev4 ? false
        add dev4
        contain dev5 ? false
        add dev5
        contain dev1 ? true
        contain dev2 ? true
        contain dev3 ? true
        contain dev4 ? true
        contain dev5 ? true
        contain dev6 ? false
        add dev6
        contain dev7 ? false
        add dev7
*/