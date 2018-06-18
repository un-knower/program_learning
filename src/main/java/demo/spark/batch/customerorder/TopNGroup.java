package demo.spark.batch.customerorder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 分组排序取Top N
 * 插入排序
 */
public class TopNGroup {

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TopNGroup").setMaster("local[1]");
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

        JavaPairRDD<String, Integer> studentScoresRdd = jsc.parallelizePairs(studentScores);
        studentScoresRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + ":" + t._2);
            }
        });

        JavaPairRDD<String, Iterable<Integer>> classScoresRdd = studentScoresRdd.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> top3Result = classScoresRdd.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {

            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t)
                    throws Exception {
                final int topN = 3;
                Integer[] top3 = new Integer[topN];
                String className = t._1;
                Iterator<Integer> scores = t._2.iterator();

                /*
                 * 找出scores中最大的3个数字
                 */
                while (scores.hasNext()) {
                    int score = scores.next();
                    for (int i = 0; i < topN; i++) {
                        if (top3[i] == null) {
                            top3[i] = score;
                            break;  //break for
                        } else if (top3[i] < score) {
                            int scoreCopy = score;
                            score = top3[i];
                            top3[i] = scoreCopy;
                        }
                    }//for
                }//while
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }

        });

        //打印输出结果
        top3Result.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.print("\nclass name=" + t._1);
                System.out.print("\t scores=");
                Iterator<Integer> ite = t._2.iterator();
                while (ite.hasNext()) {
                    System.out.print("\t" + ite.next());
                }
            }
        });


        jsc.stop();

    }
}
