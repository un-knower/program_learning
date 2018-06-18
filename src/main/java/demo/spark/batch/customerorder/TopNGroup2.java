package demo.spark.batch.customerorder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;
/**
 * 分组排序取Top N
 * 快速排序（如果数据量太大了，在排序过程中撑爆内存，需要使用插入排序）
 */
public class TopNGroup2 {
    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TopNGroup").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        List<Tuple2<String, Integer>> studentScores = Arrays.asList(
                new Tuple2<String, Integer>("class1", 78),
                new Tuple2<String, Integer>("class1", 55),
                new Tuple2<String, Integer>("class2", 90),
                new Tuple2<String, Integer>("class1", 60),
                new Tuple2<String, Integer>("class3", 66),
                new Tuple2<String, Integer>("class2", 68),
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class1", 80)
        );

        JavaRDD<Tuple2<String, Integer>> studentScoresRdd = jsc.parallelize(studentScores);
        studentScoresRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + ":" + t._2);
            }
        });


        JavaPairRDD<String, Iterable<Integer>> classScoresRdd = studentScoresRdd.mapToPair(v -> new Tuple2<>(v._1, v._2)).groupByKey();
        JavaPairRDD<String, Iterator<Integer>> stringIteratorJavaPairRDD = classScoresRdd.mapValues(new Function<Iterable<Integer>, Iterator<Integer>>() {
             @Override
             public Iterator<Integer> call(Iterable<Integer> scores) throws Exception {

                 Iterator<Integer> scoreIter = scores.iterator();
                 List<Integer> list = new ArrayList<>();
                 while (scoreIter.hasNext()) {
                     Integer next = scoreIter.next();
                     list.add(next);
                 }

                 Collections.sort(list, new Comparator<Integer>() {
                     @Override
                     public int compare(Integer o1, Integer o2) {
                         return -(o1 - o2);
                     }
                 });
                 if (list.size() > 2) {
                     list = list.subList(0, 3);  // 取Top3
                 }
                 return list.iterator();
             }
         }
        );

        stringIteratorJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Iterator<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterator<Integer>> stringIteratorTuple2) throws Exception {
                Iterator<Integer> integerIterator = stringIteratorTuple2._2;
                while(integerIterator.hasNext()) {
                    System.out.println(stringIteratorTuple2._1+"=>"+integerIterator.next());
                }
            }
        });

        jsc.stop();
    }

}
