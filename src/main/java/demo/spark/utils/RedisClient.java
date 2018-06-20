package demo.spark.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * redis sparkstreaming
 */
public class RedisClient implements KryoSerializable {
    public JedisPool jedisPool;
    public String host;
    public Integer port;
    public String password;

    public RedisClient(){
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
    }

    public RedisClient(String host){
        System.out.println("initial redis "+host);
        this.host = host;
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        jedisPool = new JedisPool(new GenericObjectPoolConfig(), host);
    }
    public RedisClient(String host, int port){
        System.out.println("initial redis "+host+":"+port);
        this.host = host;
        this.port = port;
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        jedisPool = new JedisPool(new GenericObjectPoolConfig(), host, port);
    }
    public RedisClient(String host, int port, String passport){
        System.out.println("initial redis "+host+":"+port+" "+passport);
        this.host = host;
        this.port = port;
        this.password = passport;
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(false);
        config.setMaxTotal(1000);
        jedisPool = new JedisPool(config, host, port, 100000, passport);
    }

    class CleanWorkThread extends Thread{
        @Override
        public void run() {
            System.out.println("Destroy jedis pool");
            if (null != jedisPool){
                jedisPool.destroy();
                jedisPool = null;
            }
        }
    }

    public Jedis getResource(){
        return jedisPool.getResource();
    }

    public void returnResource(Jedis jedis){
        jedisPool.returnResource(jedis);
    }

    /**
     * 好像这里对spark序列化 不相关
     * @param kryo
     * @param output
     */
    @Override
    public void write(Kryo kryo, Output output) {
//        System.out.println("Write");
//        kryo.writeObject(output, "");

    }


    /**
     * 好像这里对spark序列化 不相关
     * @param kryo
     * @param input
     */
    @Override
    public void read(Kryo kryo, Input input) {
//        System.out.println("Read");
//        host = kryo.readObject(input, String.class);
//        jedisPool =new JedisPool(new GenericObjectPoolConfig(), host) ;
    }
}