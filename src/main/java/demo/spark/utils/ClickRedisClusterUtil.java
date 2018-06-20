package demo.spark.utils;//package utils;
//
//import redis.clients.jedis.HostAndPort;
//import redis.clients.jedis.JedisCluster;
//import redis.clients.jedis.JedisPoolConfig;
//import redis.clients.jedis.Tuple;
//
//import java.io.Serializable;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
///**
// * <p>
// * redis通用工具类
// * </p>
// *
// * @author bigSea
// */
//public class ClickRedisClusterUtil implements Serializable {
//
//    private volatile static JedisCluster jedisCluster = null;
//
//    /**
//     * 创建redis cluster
//     * @param serverNodes  serverIp:serverPort,serverIp:serverPort,serverIp:serverPort
//     */
//    public static ClickRedisClusterUtil getInstance(String serverNodes) {
//        if (jedisCluster == null) {
//            String[] serverArray = serverNodes.split(",");
//            Set<HostAndPort> nodes = new HashSet<HostAndPort>();
//            for (String server : serverArray) {
//                String[] ipPortPair = server.split(":");
//                nodes.add(new HostAndPort(ipPortPair[0], Integer.valueOf(ipPortPair[1])));
//            }
//
//            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//            jedisPoolConfig.setMaxTotal(100);
//            jedisPoolConfig.setMaxIdle(12);
//            jedisPoolConfig.setMaxWaitMillis(3000L);
//            jedisPoolConfig.setTestOnBorrow(true);
//            jedisPoolConfig.setTestOnReturn(true);
//            jedisCluster = new JedisCluster(nodes, 5000, 6, jedisPoolConfig);
//            return jedisCluster;
//
//        }
//
//    }
//
//    /**
//     * 创建redis cluster
//     * @param nodes
//     */
//    public ClickRedisClusterUtil(Set<HostAndPort> nodes) {
//        if (jedisCluster == null) {
//            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//            jedisPoolConfig.setMaxTotal(35);
//            jedisPoolConfig.setMaxIdle(12);
//            jedisPoolConfig.setMaxWaitMillis(3000L);
//            jedisPoolConfig.setTestOnBorrow(true);
//            jedisPoolConfig.setTestOnReturn(true);
//            jedisCluster = new JedisCluster(nodes, 5000, 8, jedisPoolConfig);
//        }
//
//    }
//
//    /**
//     *  set String
//     * @param key
//     * @param value
//     * @return 成功：OK
//     */
//    public String setString(String key, String value) {
//        return jedisCluster.set(key, value);
//    }
//
//    /**
//     * get String
//     * @param key
//     * @return
//     */
//    public String getString(String key) {
//        return jedisCluster.get(key);
//    }
//
//    public byte[] getByte(byte[] key) {
//        return jedisCluster.get(key);
//    }
//
//    public String setEx(byte[] key, byte[] value, int seconds) {
//        return jedisCluster.setex(key, seconds, value);
//    }
//
//    /**
//     *
//     * @param key
//     * @param value
//     * @return 旧值，不存在为null
//     */
//    public String getAndSetString(String key, String value){
//        return jedisCluster.getSet(key, value);
//    }
//
//    /**
//     *
//     * @param key
//     * @param value
//     * @return
//     */
//    public Long setNx(String key, String value) {
//        return jedisCluster.setnx(key, value);
//    }
//
//    /**
//     * set not exist
//     * @param key
//     * @param value
//     * @param seconds
//     * @return
//     */
//    public String setEx(String key, String value, int seconds) {
//        return jedisCluster.setex(key, seconds, value);
//    }
//
//    /**
//     * set key, range(value, offset)
//     * @param key
//     * @param value
//     * @param offset
//     * @return
//     */
//    public Long setRange(String key, String value, long offset) {
//        return jedisCluster.setrange(key, offset, value);
//    }
//
//    /**
//     * get key range(value, startOffset, endOffset)
//     * @param key
//     * @param startOffset
//     * @param endOffset
//     * @return
//     */
//    public String getRange(String key, int startOffset, int endOffset) {
//        return jedisCluster.getrange(key, startOffset, endOffset);
//    }
//
//    /**
//     * 追加
//     * @param key
//     * @param appendStr
//     * @return len(appendStr)
//     */
//    public Long appendString(String key, String appendStr) {
//        return jedisCluster.append(key, appendStr);
//    }
//
//    /**
//     * 自增1
//     * @param key
//     * @return
//     */
//    public Long incr(String key) {
//        return jedisCluster.incr(key);
//    }
//
//    /**
//     * 自增+by
//     * @param key
//     * @param by
//     * @return
//     */
//    public Long incrBy(String key, int by) {
//        return jedisCluster.incrBy(key, by);
//    }
//
//    /**
//     * 设置值  key field value
//     * @param key
//     * @param field
//     * @param value
//     * @return
//     */
//    public Long hSet(String key, String field, String value) {
//        return jedisCluster.hset(key, field, value);
//    }
//
//    public Long hDel(String key, String field) {
//        return jedisCluster.hdel(key, field);
//    }
//
//    public String hGet(String key, String field) {
//        return jedisCluster.hget(key, field);
//    }
//    /**
//     * 判断 key field是否存在
//     * @param key
//     * @return
//     */
//    public Boolean hExists(String key, String field) {
//        return jedisCluster.hexists(key, field);
//    }
//
//    /**
//     * 查看key的field
//     * @param key
//     * @return
//     */
//    public Set<String> hKeys(String key) {
//        return jedisCluster.hkeys(key);
//    }
//
//    /**
//     * 查看key的各个field对应的值
//     * @param key
//     * @return
//     */
//    public Map<byte[], byte[]> hGetAll(byte[] key) {
//        return jedisCluster.hgetAll(key);
//    }
//    /**
//     * 查看key的各个field对应的值
//     * @param key
//     * @return
//     */
//    public Map<String, String> hGetAll(String key) {
//        return jedisCluster.hgetAll(key);
//    }
//
//    public Long hIncrBy(String key, String field, long by) {
//        return jedisCluster.hincrBy(key, field, by);
//    }
//
//    public List<String> hMget(String key, String... fields) {
//        return jedisCluster.hmget(key, fields);
//    }
//
//    public Long hDel(String key, String... fields) {
//        return jedisCluster.hdel(key, fields);
//    }
//
//    public Long hSetNx(String key, String field, String value) {
//        return jedisCluster.hsetnx(key, field, value);
//    }
//
//    public List<String> hVals(String key) {
//        return jedisCluster.hvals(key);
//    }
//
//    public Long expire(String key, int seconds) {
//        return jedisCluster.expire(key, seconds);
//    }
//
//    public Long pExpire(String key, long millisecond) {
//        return jedisCluster.pexpire(key, millisecond);
//    }
//
//
//
//    public Long lPush(String key, String... values) {
//        return jedisCluster.lpush(key, values);
//    }
//
//    /**
//     * set 操作
//     */
//    public Long sAdd(String key, String... values) {
//        return jedisCluster.sadd(key, values);
//    }
//
//    public Set<String> sMemebers(String key) {
//        return jedisCluster.smembers(key);
//    }
//
//    public Long sCard(String key) {
//        return jedisCluster.scard(key);
//    }
//
//    public Boolean sIsmember(String key, String value) {
//        return jedisCluster.sismember(key, value);
//    }
//
//    public String sRandMemeber(String key) {
//        // 返回集合中的1个随机元素
//        return jedisCluster.srandmember(key);
//    }
//
//    public List<String> sRandMemeber(String key, int count) {
//        // 返回集合中的count个随机元素
//        return jedisCluster.srandmember(key, count);
//    }
//
//    public String sPop(String key) {
//        // 移除并返回集合中的一个随机元素
//        return jedisCluster.spop(key);
//    }
//
//
//    /**
//     * sorted set 操作
//     */
//    public Long zRemRangeByScore(String key, double startScore, double endScore) {
//        return jedisCluster.zremrangeByScore(key, startScore, endScore);
//    }
//
//    public Long zRemRangeByRank(String key, long start, long end) {
//        return jedisCluster.zremrangeByRank(key, start, end);
//    }
//
//    public void zAddKeyRemRangeByScoreAdnRank(String key, double timeScore, String value, long maxZSortSetLen, long ttlMillisecond) {
//        jedisCluster.zadd(key, timeScore, value);
//        jedisCluster.pexpire(key, ttlMillisecond);
//        Long currentSetLength = jedisCluster.zcard(key);
//        if (currentSetLength > maxZSortSetLen) {
//            jedisCluster.zremrangeByRank(key, 0, -(maxZSortSetLen + 1));
//        }
//
//        long currentTimeMillis = System.currentTimeMillis();
//        jedisCluster.zremrangeByScore(key, 0d, currentTimeMillis - ttlMillisecond);
//
//
//    }
//
//    public void zremrangeByScore(String key, double start, double end) {
//        jedisCluster.zremrangeByScore(key, start, end);
//    }
//    public void zAddKeyRemRangeByScoreAdnRank(String key, Map<String, Double> memberScores, long maxZSortSetLen, long ttlMillisecond) {
//        jedisCluster.zadd(key, memberScores);
//        jedisCluster.pexpire(key, ttlMillisecond);
//        Long currentSetLength = jedisCluster.zcard(key);
//        if (currentSetLength > maxZSortSetLen) {
//            jedisCluster.zremrangeByRank(key, 0, -(maxZSortSetLen + 1));
//        }
//
//        long currentTimeMillis = System.currentTimeMillis();
//        jedisCluster.zremrangeByScore(key, 0d, currentTimeMillis - ttlMillisecond);
//    }
//
//    public long del(String key) {
//        return jedisCluster.del(key);
//    }
//
//    public void zAdd(String key, double timeScore, String value) {
//        jedisCluster.zadd(key, timeScore, value);
//    }
//    public void zAdd(String key, Map<String, Double> scoreMembers) {
//        jedisCluster.zadd(key, scoreMembers);
//    }
//
//    public Set<String> zrangeAll(String key) {
//         return jedisCluster.zrange(key, 0, -1);
//    }
//    public Set<Tuple> zrangeAllWithScores(String key) {
//        return jedisCluster.zrangeWithScores(key, 0, -1);
//    }
//
//    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
//        return jedisCluster.zrangeByScoreWithScores(key, min, max);
//    }
//
//}