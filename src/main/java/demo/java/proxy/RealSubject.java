package demo.java.proxy;

public class RealSubject implements ISubject {

  @Override
  public void rent() {
    System.out.println("I want to rent my house");
  }

  @Override
  public void hello(String str) {
    System.out.println("hello "+str);
  }
  
}
