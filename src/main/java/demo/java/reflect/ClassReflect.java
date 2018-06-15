package demo.java.reflect;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class ClassReflect {
  public static void main(String[] args) throws ClassNotFoundException {
    Class<?> forName = Class.forName("com.reflect.test.Student");
    String name = forName.getName();
    System.out.println(name);  //com.reflect.test.Student

    Field[] declaredFields = forName.getDeclaredFields();
    for(Field field : declaredFields) {
      System.out.println(field.getName());
//      id
//      name
//      score
    }
    
    Field[] fields = forName.getFields();
    for(Field field : fields) {
      System.out.println(field.getName());
    }
    
    Method[] methods = forName.getMethods();
    for(Method method : methods) {
      System.out.println(method.getName());
      System.out.println(method.getParameterCount());
      Class<?>[] parameterTypes = method.getParameterTypes();
      for(Class<?> parameterType : parameterTypes) {
        System.out.println(parameterType.getName());
      }
    }
    
  }
}
