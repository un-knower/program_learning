package demo.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class HBase16 {
	static Configuration configuration = new Configuration();
	static {
		configuration.set("hbase.zookeeper.quorum","10.10.22.11,10.10.22.15,10.10.22.16");// 使用eclipse时必须添加这个，否则无法定位
		//configuration.set("zookeeper.znode.parent","/dm-hbase-video");//使用eclipse时必须添加这个，否则无法定位。如果测试注释掉
		//configuration.set("ipc.ping.interval", "3000");
		configuration.set("hbase.zookeeper.property.clientPort", "2182");
		configuration.set("hbase.client.write.buffer", "10242880");
		configuration.set("hbase.client.retries.number", "11");
		configuration.set("hbase.client.pause", "20");
		configuration.set("hbase.security.authentication", "simple");
		configuration.set("hbase.ipc.client.tcpnodelay", "true");
		configuration.set("hbase.rootdir", "hdfs://10.10.22.16:8020/hbase");
		//configuration.set("hbase.tmp.dir", "/data/var/hbase");
		configuration.set("hbase.cluster.distributed", "true");
	}
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		HBaseAdmin admin = new HBaseAdmin(configuration);
		Get get = new Get("0039011fb3c39c62ad6f71d13fb15f85".getBytes());
		HTable htable = new HTable(configuration, "hbase_t_m_user_shortmodel_pushvideo".getBytes());
		Result result = htable.get(get);
		byte[] value = result.getValue("push".getBytes(),"vid".getBytes());
		System.out.println(new String(value));
	}
}
