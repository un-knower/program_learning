package demo.zookeeper.conf;

public interface Constance {
//	String ZK_CONNECTION_STRING = "172.21.14.25:2181";
	String ZK_CONNECTION_STRING = "192.168.1.160:2181,192.168.1.162:2181,192.168.1.163:2181";
    int ZK_SESSION_TIMEOUT = 5000;
    String ZK_REGISTRY_PATH = "/registry";
    String ZK_PROVIDER_PATH = ZK_REGISTRY_PATH + "/provider";
}
