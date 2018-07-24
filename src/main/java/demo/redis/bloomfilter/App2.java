package demo.redis.bloomfilter;


import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.BitSet;

public class App2 {
    public static void main(String args[]) {
        System.out.println(Math.log(5)/Math.log(10));
        System.out.println(Math.log10(5));

        System.out.println(Integer.MAX_VALUE);
        System.out.println((Integer.MAX_VALUE+1L)*2);

        System.out.println((int)Math.pow(2,32));
        System.out.println(Math.pow(2,32));


        for (int i=0;i<10;i++) {
            System.out.println(i%2);
        }

        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(false);
        JedisPool pool = new JedisPool(config, "192.168.1.160", 6379, 100000);
        Jedis jedis = pool.getResource();



        BitSet initBitSet1 = new BitSet(55);
        BitSet initBitSet2 = new BitSet(129);

        // https://www.cnblogs.com/xupengzhang/p/7966755.html


        //全量bitset
        BitSet allBitSet = new BitSet();
        //偶数bitset
        BitSet evenBitSet = new BitSet();
        //奇数bitset
        BitSet oddBitSet = new BitSet();
        //空bitset
        BitSet emptyBitSet = new BitSet();

        for (int i=0;i<63;i++) {
            allBitSet.set(i);
            if (i%2 == 0) {
                evenBitSet.set(i);
            } else {
                oddBitSet.set(i);
            }
        }


        System.out.println(allBitSet.cardinality());   // 63
        System.out.println("偶数Bit-true的个数：" + evenBitSet.cardinality());  // 32
        System.out.println("奇数Bit-true的个数：" + oddBitSet.cardinality());   // 31


        System.out.println(evenBitSet.get(2) +":"+ oddBitSet.get(2)); // true:false
        System.out.println("全量Bit-长度：" + allBitSet.length());   // 63
        System.out.println("偶数Bit-长度：" + evenBitSet.length());  // 63




    }

}
