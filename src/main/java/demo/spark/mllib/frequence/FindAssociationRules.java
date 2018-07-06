package demo.spark.mllib.frequence;

import demo.hadoop.frequence.Combination;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Combination.findSortedCombinations
 findSortedCombinations(List<a,b,c,d>)会生成
 [
    [],
    [a],
    [b],
    [c],
    [d],
    [a,b],
    [a,c],
    [a,d],
    [b,c],
    [c,d],
    [a,b,c],
    [a,b,d],
    [a,c,d],
    [b,c,d],
    [a,b,c,d]
 ]

 JavaPairRDD<List<String>, Integer> patterns
 ([a],1)
 ([b],1)
 ([c],1)
 ([d],1)
 ([a,b],1)
 ([a,c],1)
 ([a,d],1)
 ([b,c],1)
 ([c,d],1)
 ([a,b,c],1)
 ([a,b,d],1)
 ([a,c,d],1)
 ([b,c,d],1)
 ([a,b,c,d],1)

 JavaPairRDD<List<String>, Integer> combined  归约频繁项，相同key的进行累加





 *
 *
 *
 *
 */
public class FindAssociationRules {
    private static final String INPUT_PATH = "hdfs://SparkMaster:9000/eclipse/data/mba";
    private static final String OUTPUT_PATH = "hdfs://SparkMaster:9000/out";

    public static void main(String args[]) {

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]").setAppName(FindAssociationRules.class.getSimpleName());

        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> transactions = ctx.textFile(INPUT_PATH);
        JavaPairRDD<List<String>, Integer> patterns = transactions.flatMapToPair(new PairFlatMapFunction<String, List<String>, Integer>() {
            @Override
            public Iterator<Tuple2<List<String>, Integer>> call(String line) throws Exception {
                String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(line, ",");
                List<String> items = new ArrayList<>();
                for (String token : tokens) {
                    items.add(token);
                }
                List<List<String>> combinations = Combination.findSortedCombinations(items);
                List<Tuple2<List<String>, Integer>> result = new ArrayList<>();
                for (List<String> combList : combinations) {
                    if (combList.size() > 0) {
                        result.add(new Tuple2<>(combList, 1));
                    }
                }
                return result.iterator();
            }
        });

        JavaPairRDD<List<String>, Integer> combined = patterns.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // now, we have: patterns(K,V)
        //      K = pattern as List<String>
        //      V = frequency of pattern
        // now given (K,V) as (List<a,b,c>, 2) we will
        // generate the following (K2,V2) pairs:
        //
        //   (List<a,b,c>, T2(null, 2))
        //   (List<a,b>,   T2(List<a,b,c>, 2))
        //   (List<a,c>,   T2(List<a,b,c>, 2))
        //   (List<b,c>,   T2(List<a,b,c>, 2))


        JavaPairRDD<List<String>, Tuple2<List<String>, Integer>> subpatterns = combined.flatMapToPair(new PairFlatMapFunction<
                Tuple2<List<String>, Integer>,   // T
                List<String>,                    // key
                Tuple2<List<String>, Integer>    // value
                >() {
            @Override
            public Iterator<Tuple2<List<String>, Tuple2<List<String>, Integer>>> call(Tuple2<List<String>, Integer> pattern) throws Exception {
                List<Tuple2<List<String>, Tuple2<List<String>, Integer>>> result = new ArrayList<>();
                List<String> list = pattern._1;
                Integer frequency = pattern._2;
                result.add(new Tuple2<>(list, new Tuple2<>(null, frequency)));
                if (list.size() == 1) {
                    return result.iterator();
                }

                // 模式中包含多个商品
                for (int i = 0; i < list.size(); i++) {
                    // list remove one item
                    List<String> cloned = new ArrayList<>(list);
                    cloned.remove(i);
                    result.add(new Tuple2<>(cloned, new Tuple2<>(list, frequency)));
                }
                return result.iterator();
            }
        });
        subpatterns.saveAsTextFile(OUTPUT_PATH+"/subpatterns");

        JavaPairRDD<List<String>, Iterable<Tuple2<List<String>, Integer>>> rules = subpatterns.groupByKey();
        rules.saveAsTextFile(OUTPUT_PATH+"/rules");

        // Now, use (K=List<String>, V=Iterable<Tuple2<List<String>,Integer>>)
        // to generate association rules
        // JavaRDD<R> map(Function<T,R> f)
        // Return a new RDD by applying a function to all elements of this RDD.
        JavaRDD<List<Tuple3<List<String>,List<String>,Double>>> assocRules = rules.map(new Function<
                        Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>>,     // T: input
                        List<Tuple3<List<String>,List<String>,Double>>                   // R: ( ac => b, 1/3): T3(List(a,c), List(b),  0.33)
                        //    ( ad => c, 1/3): T3(List(a,d), List(c),  0.33)
                        >() {
            @Override
            public List<Tuple3<List<String>,List<String>,Double>> call(Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>> in) {
                List<Tuple3<List<String>,List<String>,Double>> result =
                        new ArrayList<Tuple3<List<String>,List<String>,Double>>();
                List<String> fromList = in._1;
                Iterable<Tuple2<List<String>,Integer>> to = in._2;
                List<Tuple2<List<String>,Integer>> toList = new ArrayList<Tuple2<List<String>,Integer>>();
                Tuple2<List<String>,Integer> fromCount = null;
                for (Tuple2<List<String>,Integer> t2 : to) {
                    // find the "count" object
                    if (t2._1 == null) {
                        fromCount = t2;
                    }
                    else {
                        toList.add(t2);
                    }
                }

                // Now, we have the required objects for generating association rules:
                //  "fromList", "fromCount", and "toList"
                if (toList.isEmpty()) {
                    // no output generated, but since Spark does not like null objects, we will fake a null object
                    return result; // an empty list
                }

                // now using 3 objects: "from", "fromCount", and "toList",
                // create association rules:
                for (Tuple2<List<String>,Integer>  t2 : toList) {
                    double confidence = (double) t2._2 / (double) fromCount._2;
                    List<String> t2List = new ArrayList<String>(t2._1);
                    t2List.removeAll(fromList);
                    result.add(new Tuple3(fromList, t2List, confidence));
                }
                return result;
            }
        });
        assocRules.saveAsTextFile(OUTPUT_PATH+"/assocRules");

        // done
        ctx.close();

        System.exit(0);

    } // main
}


/*
        [([b, c],[a],0.5), ([b, c],[d],1.0)]
        [([a, d],[c],1.0), ([a, d],[b],1.0)]
        [([a, c, d],[b],1.0)]
        [([b],[c],0.6666666666666666), ([b],[a],0.6666666666666666), ([b],[d],0.6666666666666666)]
        [([a, b],[c],0.5), ([a, b],[d],0.5)]
        [([c],[b],1.0), ([c],[a],0.5), ([c],[d],1.0)]
        [([b, d],[c],1.0), ([b, d],[a],0.5)]
        [([a, b, c],[d],1.0)]
        [([b, c, d],[a],0.5)]
        [([a, b, d],[c],1.0)]
        [([d],[a],0.5), ([d],[b],1.0), ([d],[c],1.0)]
        [([a],[d],0.3333333333333333), ([a],[b],0.6666666666666666), ([a],[c],0.3333333333333333)]
        [([a, c],[d],1.0), ([a, c],[b],1.0)]
        []
        [([c, d],[a],0.5), ([c, d],[b],1.0)]
*/