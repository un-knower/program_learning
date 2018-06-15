package demo.zookeeper.java.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
////UnicastRemoteObject用于导出的远程对象和获得与该远程对象通信的存根。
public class ServiceImpl extends UnicastRemoteObject implements IService {


	public ServiceImpl() throws RemoteException {

	}


	@Override
	public String sayHello(String name) throws RemoteException {
		return String.format("server >> Hello %s", name);
	}

}
