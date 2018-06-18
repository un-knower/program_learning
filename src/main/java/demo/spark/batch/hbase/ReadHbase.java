package demo.spark.batch.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
/**


 /usr/local/spark/spark-1.5.1/bin/spark-submit \
 --class com.spark.hbase.ReadHbase \
 --master spark://SparkMaster:7077 \
 /data/spark-hbase/spark-hbase.jar


 *
 */
public class ReadHbase {
    public static final String TABLE_NAME="scores";
    public static final String FAMILY_NAME1="course";
    public static final String COLUME_NAME1="art";
    public static final String ROW_KEY="Tom";
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("read hbase");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.rootdir", "hdfs://SparkMaster:9000/hbase");
        configuration.set("hbase.zookeeper.quorum", "SparkMaster,SparkWorker1,SparkWorker2");
        configuration.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(FAMILY_NAME1));
        scan.addColumn(Bytes.toBytes(FAMILY_NAME1), Bytes.toBytes(COLUME_NAME1));

        try {
            ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
            String scanToString  = Base64.encodeBytes(proto.toByteArray());
            configuration.set(TableInputFormat.SCAN, scanToString);

        } catch (IOException e) {
            e.printStackTrace();
        }
        JavaPairRDD<ImmutableBytesWritable, Result> rdd = sc.newAPIHadoopRDD(configuration,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class);


        System.out.println("总个数："+rdd.count()); //



    }
}

