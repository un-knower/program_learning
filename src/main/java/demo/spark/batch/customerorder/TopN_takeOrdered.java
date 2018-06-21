package demo.spark.batch.customerorder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * top方法底层使用的是takeOrdered
 */
public class TopN_takeOrdered {
    private static final int N = 5;
    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("top n").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<String> data = new ArrayList<>();
        data.add("A,2");
        data.add("B,2");
        data.add("C,3");
        data.add("D,2");
        data.add("E,1");
        data.add("G,2");
        data.add("A,1");
        data.add("B,1");
        data.add("C,3");
        data.add("E,1");
        data.add("F,1");
        data.add("G,2");
        data.add("A,2");
        data.add("B,2");
        data.add("C,1");
        data.add("D,2");
        data.add("E,1");
        data.add("F,1");
        data.add("G,2");

        JavaRDD<String> dataRdd = jsc.parallelize(data);
        JavaPairRDD<String, Integer> urlAndCount = dataRdd.mapToPair(v -> {
            String[] split = v.split(",");
            return new Tuple2<>(split[0], Integer.parseInt(split[1]));
        }).reduceByKey((v1, v2) -> v1+v2);


        urlAndCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

        System.out.println("top n: takeOrdered");
        // 取 bottom n
        List<Tuple2<String, Integer>> result = urlAndCount.takeOrdered(N, new MyTupleComparator());

        for (Tuple2<String, Integer> r : result) {
            System.out.println(r);
        }

        System.out.println("top n: top");
        List<Tuple2<String, Integer>> result2 = urlAndCount.top(N, new MyTupleComparator());
        for (Tuple2<String, Integer> r : result) {
            System.out.println(r);
        }




        jsc.stop();

    }


    static class MyTupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

        @Override
        public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
            //return o1._2.compareTo(o2._2); // bottom n
            return o1._2-o2._2; // bottom n
            //return -o1._2.compareTo(o2._2); // top n
        }
    }


}

/*

        (G,6)
        (B,5)
        (F,2)
        (D,4)
        (A,5)
        (C,7)
        (E,3)
        top n: takeOrdered
        (F,2)
        (E,3)
        (D,4)
        (A,5)
        (B,5)
        top n: top
        (F,2)
        (E,3)
        (D,4)
        (A,5)
        (B,5)

        */