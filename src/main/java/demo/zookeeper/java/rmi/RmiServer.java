package demo.zookeeper.java.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

public class RmiServer {
	public static void main(String[] args) throws RemoteException, MalformedURLException {
		int port = 1099;
		String packagename = "rmi.ServiceImpl";
		String url = "rmi://localhost:"+port+"/"+packagename;
		 //本地主机上的远程对象注册表Registry的实例，并指定端口
		LocateRegistry.createRegistry(port);
		//把远程对象注册到RMI注册服务器上
		Naming.rebind(url, new ServiceImpl());
	}
}
