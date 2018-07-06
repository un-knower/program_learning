package demo.spark.structed_streaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

/**
 * Author: wguangliang
 * Date: 2018/4/2
 * Description:
 */
public class ExposeAndClickWithoutJoin {
    private static final Logger logger = LoggerFactory.getLogger(ExposeAndClickWithoutJoin.class);

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("test").master("local[4]")
                .config("spark.sql.shuffle.partitions",3)
                .config("spark.default.parallelism",3)
                .getOrCreate();

        Dataset<Row> expose = spark.readStream().format("socket")
                .option("host","192.168.43.129")
                .option("port","61516")
                .load();

        StructType type = new StructType().add("time","timestamp")
                .add("docid","string")
                .add("type","string");

        expose.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                String[] args = value.mkString().split(" ");
                Object[] params = new Object[3];
                params[0]= new Timestamp(Long.parseLong(args[0]));
                params[1] = args[1];
                params[2] = args[2];

                Row newRow = new GenericRowWithSchema(params,type);
                return newRow;
            }
        }, RowEncoder.apply(type)).withWatermark("time","5 minutes").createOrReplaceTempView("expose");

        StreamingQuery q =
                spark.sql("select e1.time, e1.docid,e1.type,e2.time,e2.type from expose e1 inner join expose e2 on e1.docid=e2.docid and e1.type='1' and e2.type='2' ")
                        .writeStream().outputMode("append")
                        .format("console").trigger(Trigger.ProcessingTime(10000)).start();

        q.awaitTermination();
    }
}
