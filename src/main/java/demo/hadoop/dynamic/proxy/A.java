package demo.hadoop.dynamic.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class A implements CalculationProtocol{

	@Override
	public int add(int a, int b) {
		return a+b;
	}

	@Override
	public int sub(int a, int b) {
		return a-b;
	}
	public static void main(String[] args) {
		A a = new A();
		InvocationHandler handler = new CalculationHandler(a);
		CalculationProtocol a_proxy = (CalculationProtocol)Proxy.newProxyInstance(a.getClass().getClassLoader(), a.getClass().getInterfaces(), handler);
		System.out.println(a_proxy.add(1, 2));
//		befor call invoike
//		after call invoike
//		3
		System.out.println(a.add(1, 2));
//		3
	}
}

class CalculationHandler implements InvocationHandler {
	private Object originalObj;
	
	public CalculationHandler(Object obj) {
		this.originalObj = obj;
	}
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		System.out.println("befor call invoike");
		Object res = method.invoke(originalObj, args);
		System.out.println("after call invoike");
		
		return res;
	}
	
}
