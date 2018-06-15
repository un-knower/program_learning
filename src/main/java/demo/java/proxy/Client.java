package demo.java.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

public class Client {
  public static void main(String[] args) {
    //我们要代理的真实对象
    ISubject realSubject = new RealSubject();
    
    //我们要代理哪个真实对象，就将该对象传进去，最后是通过该真实对象调用其方法的
    InvocationHandler handler = new DynamicProxy(realSubject);
    
    ISubject subject = (ISubject)Proxy.newProxyInstance(handler.getClass().getClassLoader(), realSubject.getClass().getInterfaces(), handler);
    
    System.out.println(subject.getClass().getName());
    subject.rent();
    subject.hello("world");
    
  }
}
