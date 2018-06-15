package demo.java.reflect;

import com.alibaba.fastjson.JSON;

public class Jsonstr2Object {
  public static void main(String args[]) {
    Student p = new Student("D01","tom",1.05d);
    String jsonString = JSON.toJSONString(p);
    System.out.println(jsonString);
    
    Student student = new Student(jsonString);
    System.out.println(student.getId());
    
  }
}
