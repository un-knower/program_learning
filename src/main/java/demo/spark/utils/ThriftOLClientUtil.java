package demo.spark.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * thrift rpc onlinelearning
 */
public class ThriftOLClientUtil implements KryoSerializable {

    public static TTransport transport;
    public static TMultiplexedProtocol service = null;
    static {
        try {
            if (transport == null) {

                transport = new TFramedTransport(new TSocket("10.200.129.170", 9000, 300000));
                //编码协议要和服务端一致
                TProtocol protocol = new TBinaryProtocol(transport);

                service = new TMultiplexedProtocol(protocol, "FeatureExtract Service");

                System.out.println("open...................................................................................");
                if (!transport.isOpen()) {
                    transport.open();
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static TMultiplexedProtocol getService() {
        return service;
    }

    @Override
    public void write(Kryo kryo, Output output) {

    }

    @Override
    public void read(Kryo kryo, Input input) {

    }


}
