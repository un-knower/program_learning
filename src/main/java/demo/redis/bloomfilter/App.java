package demo.redis.bloomfilter;

import com.github.wxisme.bloomfilter.bitset.RedisBitSet;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import com.github.wxisme.bloomfilter.common.BloomFilter;

/**
 * 唯一需要注意的是redis的bitmap只支持2^32大小，对应到内存也就是512MB,数组的下标最大只能是2^32-1。
 * 不过这个限制我们可以通过构建多个redis的bitmap通过hash取模的方式分散一下即可。
 */
public class App {
    public static void main(String args[]) throws InterruptedException {
        //Jedis jedis , String name

        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(false);
        JedisPool pool = new JedisPool(config, "192.168.1.160", 6379, 100000);
        Jedis jedis = pool.getResource();
        RedisBitSet redisBitSet0 = new RedisBitSet(jedis, "test");
        RedisBitSet redisBitSet1 = new RedisBitSet(jedis, "test2");


        double falsePositiveProbability = 0.000001d;
        int expectedNumberOElements = Integer.MAX_VALUE;
        BloomFilter bloomFilter0 = new BloomFilter(falsePositiveProbability, expectedNumberOElements);
        bloomFilter0.bind(redisBitSet0);
        BloomFilter bloomFilter1 = new BloomFilter(falsePositiveProbability, expectedNumberOElements);
        bloomFilter1.bind(redisBitSet1);

        for (int i=0;i<3;i++) {
            if (i%2 ==0) {  // 因为是两个redis bitmap
                System.out.println(i+":"+bloomFilter0.contains(i));
            } else {
                System.out.println(i+":"+bloomFilter1.contains(i));

            }
        }

//
//        for (int i=0;i<10000;i++) {
//
//            //jedis = pool.getResource();
//
//            System.out.println("add "+i);
//            if (i%2==0) {
//                bloomFilter0.add(i);
//            } else {
//                bloomFilter1.add(i);
//            }
//            pool.returnResource(jedis);
//            //System.out.println(jedis == null);
//
//        }

//        for (int i=0;i<10000;i++) {
//            jedis = pool.getResource();
//            System.out.println("check "+i);
//            if (i%2==0) {
//                boolean trueContain = bloomFilter0.contains(i);
//                if (!trueContain) {
//                    System.out.println("should contain");
//                }
//            } else {
//                boolean trueContain = bloomFilter1.contains(i);
//                if (!trueContain) {
//                    System.out.println("should contain");
//                }
//            }
//            pool.returnResource(jedis);
//        }
//
//        for (int i=10000;i<100000;i++) {
//            jedis = pool.getResource();
//            System.out.println("check "+i);
//            if (i%2==0) {
//                boolean trueContain = bloomFilter0.contains(i);
//                if (trueContain) {
//                    System.out.println("should not contain");
//                }
//            } else {
//                boolean trueContain = bloomFilter1.contains(i);
//                if (trueContain) {
//                    System.out.println("should not contain");
//                }
//            }
//            pool.returnResource(jedis);
//        }
//
//        System.out.println("sleeping...");
//        Thread.sleep(200000);
//
//        for (int i=9990;i<20000;i++) {
//            if (i%2==0) {
//                boolean trueContain = bloomFilter0.contains(i);
//                System.out.println(trueContain);
//            } else {
//                boolean trueContain = bloomFilter1.contains(i);
//                System.out.println(trueContain);
//            }
//        }
    }

}
