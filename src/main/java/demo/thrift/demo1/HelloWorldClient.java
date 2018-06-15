package demo.thrift.demo1;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;


public class HelloWorldClient {
  public static final String SERVER_IP = "localhost";
  public static final int SERVER_PORT = 8090;
  public static final int TIMEOUT = 30000;
  
  public void startClient(String userName) {
    TTransport transport = null;
    try {
    transport = new TSocket(SERVER_IP, SERVER_PORT, TIMEOUT);
    //编码协议要和服务端一致
    
    
    TProtocol protocol = new TBinaryProtocol(transport);
    // TProtocol protocol = new TCompactProtocol(transport);
    // TProtocol protocol = new TJSONProtocol(transport);
    HelloWorldService.Client client = new HelloWorldService.Client(protocol);
    
    transport.open();
    String result = client.sayHello(userName);
    System.out.println("Thrify client result =: " + result);
    }catch (Exception e) {
      e.printStackTrace();
    } finally {
      if(transport != null) {
        transport.close();
      }
    }
    
    
  }
  public static void main(String[] args) {
    HelloWorldClient client = new HelloWorldClient();
    client.startClient("abc");
  }
}
