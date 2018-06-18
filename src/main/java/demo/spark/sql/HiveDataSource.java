package demo.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataSource {
    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setAppName("hive source");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        HiveContext hiveContext = new HiveContext(jsc);

        hiveContext.sql("DROP IF EXISTS TABLE student_infos");
        hiveContext.sql("CREATE IF NOT EXISTS TABLE student_infos (name STRING, age INT)");
        hiveContext.sql("LOAD DATA LOCAL INPATH '/usr/local/sparkdata/student_info.txt'");

        Dataset<Row> result = hiveContext.sql("select * from student_info");

        //将dataframe保存到 student_infos2
        hiveContext.sql("DROP IF EXISTS TABLE student_infos2");
        result.registerTempTable("student_infos2");

        //直接加载表数据
        Dataset<Row> result2 = hiveContext.table("student_infos2");

        //打印输出
        result2.show();
//        Row[] students = result2.collect();
//        for(Row student : students) {
//            System.out.println(student);
//        }

        jsc.close();



    }
}

