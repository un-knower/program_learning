package demo.java.reflect;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class Student {
  private String id;
  private String name;
  private Double score;
  public Student(String studentJson) {
    JSONObject jsonObject = JSON.parseObject(studentJson);
    Method[] methods = this.getClass().getDeclaredMethods();
    if(methods != null) {
      for(Method m : methods) {
        String methodName = m.getName();
        if(methodName.startsWith("set")) {
          methodName = methodName.substring(3);
          // 获取属性名称
          methodName = methodName.substring(0, 1).toLowerCase()+methodName.substring(1);
          if(!methodName.equalsIgnoreCase("class")) {
            try {
              m.invoke(this, new Object[]{jsonObject.get(methodName)});
            } catch (IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
              try {
                switch (m.getParameterTypes()[0].getName()) {
                  case "java.lang.Long" : {
                    m.invoke(this, new Object[]{Long.valueOf(jsonObject.get(methodName).toString())});
                    break;
                }
                case "java.lang.Integer" : {
                    m.invoke(this, new Object[]{Integer.valueOf(jsonObject.get(methodName).toString())});
                    break;
                }
                case "java.lang.Double" : {
                    m.invoke(this, new Object[]{Double.valueOf(jsonObject.get(methodName).toString())});
                    break;
                }
                case "java.lang.Float" : {
                    m.invoke(this, new Object[]{Float.valueOf(jsonObject.get(methodName).toString())});
                    break;
                }
                default: {
                    //TODO
                }

                }
              }catch (Exception ee) {
                ee.printStackTrace();
              }
            }
          }
        }
      }
    }
    
    
  }
  public Student(String id, String name, Double score) {
    this.id = id;
    this.name = name;
    this.score = score;
  }
 
  public String getId() {
    return id;
  }
 
  public void setId(String id) {
    this.id = id;
  }
 
  public String getName() {
    return name;
  }
  
  public void setName(String name) {
    this.name = name;
  }
  
  public Double getScore() {
    return score;
  }
 
  public void setScore(Double score) {
    this.score = score;
  }
  
}
