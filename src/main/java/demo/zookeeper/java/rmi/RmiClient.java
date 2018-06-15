package demo.zookeeper.java.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * 客户端调用远程对象上的远程方法
 * @author qingjian
 *
 */
public class RmiClient {
	public static void main(String[] args) throws MalformedURLException, RemoteException, NotBoundException {
		int port = 1099;
		String packagename = "rmi.ServiceImpl";
		IService helloService = (IService)Naming.lookup("rmi://localhost:"+port+"/"+packagename);
		//IService helloService = (IService)Naming.lookup("rmi://172.21.14.85:11211/rmi.ServiceImpl");
		String result = helloService.sayHello("zyy");
		System.out.println("client receive --> "+result);
	}
}
