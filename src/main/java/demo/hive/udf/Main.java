package demo.hive.udf;

import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;


public class Main {
  private static String algorithm = "AES";
  private static String decryptDidKy = "netxxxxnewsboard";

 
  public static void main(String[] args) throws Exception {
    Cipher cipher = null;
    byte[] raw = decryptDidKy.getBytes("ASCII");
    SecretKeySpec kySpec = new SecretKeySpec(raw, algorithm);
    cipher = Cipher.getInstance(algorithm);
//    cipher.init(Cipher.ENCRYPT_MODE, kySpec);
    cipher.init(Cipher.ENCRYPT_MODE, kySpec);
    
    
//    String devId = "hZ2vZd2w5VW/VIHgs7P0tupbYjECcV+d4u0PZqj1pcJv4SfWi1gA9k6D1+4vcoWH";
    
//    String devIdAes = new String(cipher.doFinal(Base64.getDecoder().decode(devId)));
    String devId = "CQljNDQ0YWMyMjY5YjA5NGUwCTNTQzdOMTZCMjkwMDA3OTI%3D";
    String devIdAes = org.apache.commons.codec.binary.Base64.encodeBase64String(cipher.doFinal(devId.getBytes("ASCII"))).trim();
     
    System.out.println("====:"+devIdAes); 
    System.out.println(Math.abs(devIdAes.hashCode() % 100));
    
    cipher.init(Cipher.DECRYPT_MODE, kySpec);
 
    String devIdFromAes = new String(cipher.doFinal(Base64.getDecoder().decode(devIdAes)));
    System.out.println(devIdFromAes);
    
    
    
    System.out.println(Math.abs("wba3R17Chg3uSh0yRUPzYyvEZ285Sch03G9O8sKOZ5zN2lb7A7N9BD+tWHcnSd3XIIGNeE0nI41SFrBIaL1THA==".hashCode()%100));
    
     
    
    
  }
}
