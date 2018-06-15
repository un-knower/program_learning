package demo.java;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

public class StringUtilsTets {
  @Test
  public void testIsBlank() {
    System.out.println(StringUtils.isBlank("\t")); //true
    System.out.println(StringUtils.isBlank(""));   //true
    System.out.println(StringUtils.isBlank(null));  //true
    System.out.println(StringUtils.isBlank(" A test ")); //false
  }
  
  @Test
  public void testIsEmpty() {
    System.out.println(StringUtils.isEmpty(""));    //true
    System.out.println(StringUtils.isEmpty(null));  //true
    System.out.println(StringUtils.isEmpty("\t"));  //false
    System.out.println(StringUtils.isEmpty("a test")); //false
    
  }
  
  @Test
  public void testStrip() {
    System.out.println(StringUtils.strip(" abcyx", "xyz")); //" abc"
  }
  
  @Test
  public void testContainsAny() {
    System.out.println(StringUtils.containsAny("abcdefg", "a","z")); //true
    System.out.println(StringUtils.containsAny("abcdefg", "ac","z")); //false
    System.out.println(StringUtils.containsAny("abcdefg", "de","z")); //true
  }
  
  @Test
  public void testTrim() {
    System.out.println(StringUtils.trim(" ab cd efg ")); //ab cd efg
  }
  @Test
  public void testDeleteWhitespace() {
    System.out.println(StringUtils.deleteWhitespace(" ab cd efg ")); //abcdefg
  }
  @Test
  public void testIsNumeric() {
    System.out.println(StringUtils.isNumeric("111"));  //true
    System.out.println(StringUtils.isNumeric("11 1"));  //false
    System.out.println(StringUtils.isNumeric("a"));  //false
    System.out.println(StringUtils.isNumeric("1a"));  //false
  }
  @Test
  public void testIsNumericSpace() {
    System.out.println(StringUtils.isNumericSpace("111"));  //true
    System.out.println(StringUtils.isNumericSpace("11 1"));  //true
    System.out.println(StringUtils.isNumericSpace("a"));  //false
    System.out.println(StringUtils.isNumericSpace("1a"));  //false
  }
  
  
//  @Test
//  public void testGenericJson() {
//    List<String> list = new ArrayList<>();
//    list.add("{ \"name\":\"green\", \"age\":26, \"salary\":2000, \"team\":\"war\", \"position\":\"pf\" }");
//    list.add("{ \"name\":\"red\", \"age\":26, \"salary\":3000, \"team\":\"war2\", \"position\":\"pf2\" }");
//    list.add("{ \"name\":\"yellow\", \"age\":27, \"salary\":4000, \"team\":\"war3\", \"position\":\"pf3\" }");
//    
//    System.out.println(StringUtils.wrap(StringUtils.join(list,","), "[" ));
//    
//  }
  
   @Test
   public void testCountMatches() {
     System.out.println(StringUtils.countMatches( "Chinese People", "e"));  //4
     System.out.println(StringUtils.countMatches( "ChinEsE People", "e"));  //2
   }
   @Test
   public void testReplace() {
     System.out.println(StringUtils.replace( "Chinese People", "e","E"));  //ChinEsE PEoplE

   }
  
  
  
  
  
}
