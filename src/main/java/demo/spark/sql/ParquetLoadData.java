package demo.spark.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
/**
 *
 * SparkSqlParquet数据源的加载
 *
 * @author qingjian
 *
 */
public class ParquetLoadData {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf();
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //读取Parquet文件中的数据，创建一个DataFrame
        Dataset<Row> userDf = sqlContext.read().parquet("hdfs://sparkmaster:9000/spark-study/users.parquet");

        //将DataFrame注册为临时表，然后使用SQL查询需要的数据
        userDf.registerTempTable("users");
        Dataset<Row> userNamesDf = sqlContext.sql("select name from users");

        //对查询出来的DataFrame进行Transformation操作，处理数据，然后打印出来
        List<String> names = userNamesDf.javaRDD().map(new Function<Row, String>() {
            /**
             *
             */
            private static final long serialVersionUID = 1L;

            @Override
            public String call(Row row) throws Exception {
                return "name: "+row.getString(0);
            }

        }).collect();


        for(String name : names) {
            System.out.println(name);
        }

    }
}

