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
import org.apache.hadoop.io.IntWritable;
import org.apache.storm.guava.io.BaseEncoding;

/**
 * 
 * @author bjwangguangliang
add jar /home/appops/devid_hashcode.jar;
add jar /home/appops/guava-23.0.jar;
create temporary function devid_hashcode as 'demo.hive.udf.NetxxxxHashCodeSpark';
use streams;
select devid_hashcode('E67B792E-C735-4A96-95A5-EC27FE05E70F') from streams.dw_exp_etl limit 1;
 */
public class NetxxxxHashCodeSpark extends GenericUDF {
  private String algorithm = "AES";
  private String decryptDidKy = "key_passport";
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }
  
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments.length < 1) {
      return null;
    }
    String devId = arguments[0].get().toString();
    Cipher cipher = null;
    try {
      byte[] raw = decryptDidKy.getBytes("ASCII");
      SecretKeySpec kySpec = new SecretKeySpec(raw, algorithm);
      cipher = Cipher.getInstance(algorithm);
      cipher.init(Cipher.ENCRYPT_MODE, kySpec);
      String devIdAes = BaseEncoding.base64().encode(cipher.doFinal(devId.getBytes("ASCII")));
      return new IntWritable(Math.abs(devIdAes.hashCode() % 100));
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
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "error:"+children[0];
  }
}
