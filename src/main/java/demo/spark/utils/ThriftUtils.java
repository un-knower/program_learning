package demo.spark.utils;

import com.alibaba.fastjson.util.Base64;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TMemoryInputTransport;

public class ThriftUtils {

    public static String base64Pre(String line) {
        int missing_padding = 4 - line.length() % 4;
        while (missing_padding > 0) {
            line += "=";
            missing_padding--;
        }
        return line;
    }

    /**
     * 解密base64
     *
     * @param line
     * @return
     */
    public static byte[] base64Decode(String line) {
        return Base64.decodeFast(line);
    }

    public static byte[] base64Decode(String line, boolean needBase64Pre) {
        if (needBase64Pre) {
            return Base64.decodeFast(base64Pre(line));
        } else {
            return base64Decode(line);
        }
    }

    public static TProtocol getThriftProtocol(TProtocolFactory protocolFactory, byte[] decode) {
        //TMemoryBuffer tmb = new TMemoryBuffer(0);
        return protocolFactory.getProtocol(new TMemoryInputTransport(decode));
    }



}
