package demo.spark.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * HBaseClient
 * 单例，可以广播
 */
public class HBaseClient implements KryoSerializable {

    private static Configuration conf = null;
    private static Connection conn = null;
    private static String quorum;
    private static String clientPort;

    /**
     * 获取全局唯一的Configuration实例
     *
     * @return
     */
    public static synchronized Configuration getConfiguration(String quorum, String clientPort) {
        if (conf == null) {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", quorum);
            conf.set("hbase.zookeeper.property.clientPort", clientPort);
            conf.setInt("hbase.rpc.timeout", 20000);
            conf.setInt("hbase.client.operation.timeout", 30000);
            conf.setInt("hbase.client.scanner.timeout.period", 20000);

        }
        return conf;
    }

    /**
     * 获取全局唯一的Connection实例
     *
     * @return
     * @throws ZooKeeperConnectionException
     */
    public static synchronized Connection getHConnection(String quorum, String clientPort) throws IOException {
        if (conn == null) {
            quorum = quorum;
            clientPort = clientPort;
            Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
            conn = ConnectionFactory.createConnection(getConfiguration(quorum, clientPort));
        }
        return conn;
    }

    /**
     * 获取全局唯一的HConnection实例
     *
     * @return
     * @throws ZooKeeperConnectionException
     */
    public static synchronized Connection getHConnection(Configuration conf) throws IOException {
        if (conn == null) {
            quorum = conf.get("hbase.zookeeper.quorum");
            clientPort = conf.get("hbase.zookeeper.property.clientPort");
            Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
            conn = ConnectionFactory.createConnection(conf);
        }
        return conn;
    }

    @Override
    public void write(Kryo kryo, Output output) {

        kryo.writeObject(output, quorum);
        kryo.writeObject(output, clientPort);

    }

    @Override
    public void read(Kryo kryo, Input input) {
        if (conn == null) {
            String quorum = kryo.readObject(input, String.class);
            String clientPort = kryo.readObject(input, String.class);
            Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
            try {
                conn = ConnectionFactory.createConnection(getConfiguration(quorum, clientPort));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class CleanWorkThread extends Thread {
        @Override
        public void run() {
            System.out.println("Destroy Hbase Connection");
            if (conn != null) {
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
