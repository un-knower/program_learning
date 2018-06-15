package demo.java.reflect;

import java.lang.reflect.Method;
class Person {
  private String name;
  private int age;
  public Person(String name, int age) {
    this.name = name;
    this.age = age;
  }
  public String getName() {
    return name;
  }
  public void setName(String name) {
    this.name = name;
  }
  public int getAge() {
    return age;
  }
  public void setAge(int age) {
    this.age = age;
  }
  
}
public   class   sssss2
{
    public   static   void   main(String[]   args)
    {
      
      
        Person p = new Person("aname", 111);
         
        try {
            Class c = Class.forName("com.reflect.test.Person");
            Method[] m=c.getMethods();
            for (Method me:m) {
                System.out.println(me.getName());
                System.out.println(me.invoke(p));
                
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
   
}

