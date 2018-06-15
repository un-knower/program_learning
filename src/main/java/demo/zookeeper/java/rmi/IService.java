package demo.zookeeper.java.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IService extends Remote{
	//声明服务器必须提供的服务
	String sayHello(String name) throws RemoteException;
}
