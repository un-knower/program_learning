package demo.spark.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import demo.spark.utils.bean.HelloWorldService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * thrift rpc hello service
 */
public class ThriftHelloServiceClient implements KryoSerializable {
    public HelloWorldService.Client client = null;
    public TTransport transport;
    private String serverIp;
    private Integer serverPort;
    private Integer timeOut;

    public ThriftHelloServiceClient() {
        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
    }

    public ThriftHelloServiceClient(String serverIp, int serverPort, int timeOut) {
        this.serverIp = serverIp;
        this.serverPort = serverPort;
        this.timeOut = timeOut;

        Runtime.getRuntime().addShutdownHook(new CleanWorkThread());
        try {
            if (client == null) {
                transport = new TSocket(serverIp, serverPort, timeOut);
                TProtocol protocol = new TBinaryProtocol(transport);
                client = new HelloWorldService.Client(protocol);
                System.out.println("open...");
                transport.open();

            }
        } catch (TTransportException e) {
            e.printStackTrace();
        }

    }


    @Override
    public void write(Kryo kryo, Output output) {

    }

    @Override
    public void read(Kryo kryo, Input input) {
        if (client == null) {
            transport = new TSocket(serverIp, serverPort, timeOut);
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new HelloWorldService.Client(protocol);
            System.out.println("open...");
            try {
                transport.open();
            } catch (TTransportException e) {
                e.printStackTrace();
            }

        }
    }

    class CleanWorkThread extends Thread {
        @Override
        public void run() {
            if (null != transport) {
                transport.close();
            }

        }
    }
}
