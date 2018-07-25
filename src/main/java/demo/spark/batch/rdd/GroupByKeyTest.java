package demo.spark.batch.rdd;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class GroupByKeyTest {

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<String> data = new ArrayList<>();

        data.add("1,aaa,bbb");
        data.add("1,ccc,ddd");
        data.add("1,eee,fff");
        data.add("2,aaa,bbb");
        data.add("2,ccc,ddd");
        data.add("2,eee,fff");
        JavaRDD<String> dataRdd = jsc.parallelize(data);
//    dataRdd.mapToPair(new Function<String, Tuple2<String, Tuple2<String, String>>>() {
//      @Override
//      public Tuple2<String, Tuple2<String, String>> call(String v1) throws Exception {
//        String[] split = v1.split(",");
//         return new Tuple2(split[0], new Tuple2(split[1], split[2]));
//      }
//
//    })

        JavaPairRDD<String, List<String>> result = dataRdd.mapToPair(new PairFunction<String, String, Tuple2<String, String>>() {

            @Override
            public Tuple2<String, Tuple2<String, String>> call(String t) throws Exception {
                String[] split = t.split(",");
                return new Tuple2(split[0], new Tuple2(split[1], split[2]));
            }
        }).groupByKey().mapValues(new Function<Iterable<Tuple2<String,String>>, List<String>>() {

            List<String> result = new ArrayList<>();
            @Override
            public List<String> call(Iterable<Tuple2<String, String>> v1) throws Exception {
                Iterator<Tuple2<String, String>> iterator = v1.iterator();
                result.clear();
                while(iterator.hasNext()) {
                    Tuple2<String,String> next = iterator.next();
                    result.add(next._1+" "+next._2);
                }
                return result;
            }

        });

        List<Tuple2<String, List<String>>> collect = result.collect();
        for(Tuple2<String, List<String>> t : collect) {
            List<String> next = t._2;
            for(String n:next) {
                System.out.println(t._1+":"+n);
            }

        }






    }
}
