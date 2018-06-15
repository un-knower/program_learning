package demo.thrift.demo1;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;


public class HelloWorldServer {
  public static final int SERVER_PORT = 8090;
  public void startServer() throws TTransportException {
    System.out.println("HelloWorldServer start ...");
    TProcessor tprocessor = new HelloWorldService.Processor<HelloWorldService.Iface>(new HelloWorldImpl());
    
    // 简单的单线程服务模型，一般用于测试
    TServerSocket serverSocket = new TServerSocket(SERVER_PORT);
    Args tArgs = new Args(serverSocket);
    tArgs.processor(tprocessor);
    tArgs.protocolFactory(new TBinaryProtocol.Factory()); //编码协议
    // tArgs.protocolFactory(new TCompactProtocol.Factory());
    // tArgs.protocolFactory(new TJSONProtocol.Factory());
    
    TSimpleServer server = new TSimpleServer(tArgs);
    server.serve();
  }
  public static void main(String[] args) throws TTransportException {
    HelloWorldServer server = new HelloWorldServer();
    server.startServer();
  }
}
