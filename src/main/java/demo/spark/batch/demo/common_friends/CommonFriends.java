package demo.spark.batch.demo.common_friends;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;


import java.util.*;


public class CommonFriends {
    private static final String INPUT_PATH = "hdfs://SparkMaster:9000/eclipse/data/common_friend.txt";
    public static void main(String args[]) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName(CommonFriends.class.getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> dataRdd = jsc.textFile(INPUT_PATH);
        JavaPairRDD<Tuple2<Long, Long>, List<Long>> parisRdd = dataRdd.flatMapToPair(new PairFlatMapFunction<String, Tuple2<Long, Long>, List<Long>>() {
            @Override
            public Iterator<Tuple2<Tuple2<Long, Long>, List<Long>>> call(String line) throws Exception {
                List<Tuple2<Tuple2<Long, Long>, List<Long>>> result = new ArrayList<>();
                String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, " ");
                long person = Long.parseLong(tokens[0]);
                if (tokens.length == 2) {
                    Tuple2<Long, Long> sortedTupleKey = getSortedTupleKey(person, Long.parseLong(tokens[1]));
                    result.add(new Tuple2(sortedTupleKey, new ArrayList<Long>()));
                    return result.iterator();
                } else {
                    List<Long> friendsList = new ArrayList<>();
                    for (int i = 1; i < tokens.length; i++) {
                        friendsList.add(Long.parseLong(tokens[i]));
                    }

                    for (int i = 1; i < tokens.length; i++) {
                        Tuple2<Long, Long> sortedTupleKey = getSortedTupleKey(person, Long.parseLong(tokens[i]));
                        result.add(new Tuple2<>(sortedTupleKey, friendsList));
                    }
                    return result.iterator();
                }
            }
        });

        JavaPairRDD<Tuple2<Long, Long>, Iterable<List<Long>>> groupedRdd = parisRdd.groupByKey();
        List<Tuple2<Tuple2<Long, Long>, Iterable<List<Long>>>> debug1 = groupedRdd.collect();
        for (Tuple2<Tuple2<Long, Long>, Iterable<List<Long>>> d : debug1) {
            System.out.println(d._1+"=>"+d._2);
        }
//        (300,400):[[100, 200, 400, 500], [100, 200, 300]]
//        (100,200):[[200, 300, 400, 500, 600], [100, 300, 400]]
//        (100,500):[[200, 300, 400, 500, 600], [100, 300]]
//        (200,300):[[100, 300, 400], [100, 200, 400, 500]]
//        (100,600):[[200, 300, 400, 500, 600], []]
//        (200,400):[[100, 300, 400], [100, 200, 300]]
//        (100,400):[[200, 300, 400, 500, 600], [100, 200, 300]]
//        (300,500):[[100, 200, 400, 500], [100, 300]]
//        (100,300):[[200, 300, 400, 500, 600], [100, 200, 400, 500]]

        JavaPairRDD<Tuple2<Long, Long>, List<Long>> resultRdd = groupedRdd.mapValues(new Function<Iterable<List<Long>>, List<Long>>() {
            @Override
            public List<Long> call(Iterable<List<Long>> s) throws Exception {
                Map<Long, Integer> countCommon = new HashMap<Long, Integer>();
                int numOfValues = 0;
                for (List<Long> iter : s) {
                    for (Long friend : iter) {
                        Integer count = countCommon.get(friend);
                        if (count == null) {
                            countCommon.put(friend, 1);
                        } else {
                            countCommon.put(friend, count + 1);
                        }
                    }
                    numOfValues++;
                }

                // if countCommon.Entry<f, count> ==  countCommon.Entry<f, s.size()>
                // then that is a common friend
                List<Long> finalCommonFriends = new ArrayList<Long>();
                for (Map.Entry<Long, Integer> entry : countCommon.entrySet()) {
                    if (entry.getValue() == numOfValues) {
                        finalCommonFriends.add(entry.getKey());
                    }
                }
                return finalCommonFriends;
            }
        });

        resultRdd.foreach(new VoidFunction<Tuple2<Tuple2<Long, Long>, List<Long>>>() {
            @Override
            public void call(Tuple2<Tuple2<Long, Long>, List<Long>> tuple2ListTuple2) throws Exception {
                System.out.println(tuple2ListTuple2._1 +":"+tuple2ListTuple2._2.toString());
            }
        });

//        (300,400):[100, 200]
//        (100,200):[400, 300]
//        (100,500):[300]
//        (200,300):[400, 100]
//        (100,600):[]
//        (200,400):[100, 300]
//        (100,400):[200, 300]
//        (300,500):[100]
//        (100,300):[400, 500, 200]

        jsc.stop();


    } // main
    static Tuple2<Long, Long> getSortedTupleKey(long person, long friend) {
        if (person < friend) {
            return new Tuple2<>(person, friend);
        } else {
            return new Tuple2<>(friend, person);
        }
    }
}
