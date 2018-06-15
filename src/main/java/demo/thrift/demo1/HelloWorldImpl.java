package demo.thrift.demo1;
import org.apache.thrift.TException;

public class HelloWorldImpl implements HelloWorldService.Iface {

  @Override
  public String sayHello(String username) throws TException {
    System.out.println("receive "+ username);
    return "hi, "+username+" welcome!";
  }

}
