package demo.java.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
/**
 * 动态代理类
 * @author bjwangguangliang
 *
 */
public class DynamicProxy implements InvocationHandler {

  //这个是我们要代理的真实类
  private Object subject;
  public DynamicProxy(Object subject) {
    this.subject = subject;
  }
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    
    //在代理真实对象前我们可以添加自己的操作
    System.out.println("before method");
    System.out.println("Method:"+method);
    //当代理对象调用真实对象的方法时，其会自动跳转到代理对象关联的handler对象的invoke方法来进行调用
    method.invoke(subject, args);
    System.out.println("after method");
    
    return null;
  }
  
}
