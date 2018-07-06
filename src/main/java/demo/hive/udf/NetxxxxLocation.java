package demo.hive.udf;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.storm.guava.io.BaseEncoding;

/**
 * 
 * @author wguangliang
add jar /home/appops/netxxxx_location.jar;
add jar /home/appops/guava-23.0.jar;
create temporary function netxxxx_location as 'demo.hive.udf.NetxxxxLocation';
use streams;
select netxxxx_location('ftVTBbRXKlDu26C7h4mLMA==') from streams.dw_exp_etl limit 1;
 */
public class NetxxxxLocation extends GenericUDF {
  private String algorithm = "AES";
  private String decryptDidKy = "key_passport";
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments.length < 1) {
      return null;
    }
    String location = arguments[0].get().toString();
    int padLen = 4 - location.length()%4;
    while(padLen > 0) {
      location += "=";
      padLen--;
    }
    Cipher cipher = null;
    try {
      byte[] raw = decryptDidKy.getBytes("ASCII");
      SecretKeySpec kySpec = new SecretKeySpec(raw, algorithm);
      cipher = Cipher.getInstance(algorithm);
      cipher.init(Cipher.DECRYPT_MODE, kySpec);
      
      return new Text(new String(cipher.doFinal(BaseEncoding.base64().decode(location))));
       
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (NoSuchPaddingException e) {
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    }
    return location;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "error:"+children[0];
  }
}
