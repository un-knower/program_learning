package demo.spark.mllib.recommend;

import demo.hadoop.frequence.Combination;
import demo.scala.type_parameterization.S;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple7;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MovieRecommendations {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName(MovieRecommendations.class.getSimpleName());
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> dataRdd = jsc.textFile("src/main/java/demo/spark/mllib/recommend/ratings.csv");
        dataRdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        JavaPairRDD<String, Tuple2<String, Double>> moviesRdd = dataRdd.mapToPair(new PairFunction<String, String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Tuple2<String, Double>> call(String s) throws Exception {
                String[] split = s.split(",");
                String user = split[0];
                String movie = split[1];
                double rating = Double.parseDouble(split[0]);
                Tuple2<String, Double> userAndRating = new Tuple2<>(user, rating);

                return new Tuple2<>(movie, userAndRating);
            }
        });


        JavaPairRDD<String, Iterable<Tuple2<String, Double>>> moviesGrouped = moviesRdd.groupByKey();

        //           user        <movie, rating, numberOfRatingsOfthisUser>
        JavaPairRDD<String, Tuple3<String, Double, Integer>> usersRdd = moviesGrouped.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Tuple2<String, Double>>>, String, Tuple3<String, Double, Integer>>() {
            @Override
            public Iterator<Tuple2<String, Tuple3<String, Double, Integer>>> call(Tuple2<String, Iterable<Tuple2<String, Double>>> s) throws Exception {

                List<Tuple2<String, Double>> listOfUserAndRating = new ArrayList<>();  // 遍历使用
                String movie = s._1;
                Iterable<Tuple2<String, Double>> pairsOfUserAndRating = s._2;
                int numberOfRatings = 0; // 为这部电影打分的人数
                for (Tuple2<String, Double> t2 : pairsOfUserAndRating) {
                    numberOfRatings++;
                    listOfUserAndRating.add(t2);
                }

                List<Tuple2<String, Tuple3<String, Double, Integer>>> results = new ArrayList<>();
                for (Tuple2<String, Double> t2 : listOfUserAndRating) {
                    String user = t2._1;
                    double rating = t2._2;
                    Tuple3<String, Double, Integer> t3 = new Tuple3<>(movie, rating, numberOfRatings);
                    results.add(new Tuple2<>(user, t3));
                }
                return results.iterator();
            }
        });

        JavaPairRDD<String, Iterable<Tuple3<String, Double, Integer>>> groupedbyUser = usersRdd.groupByKey();
        groupedbyUser.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<Tuple3<String,Double,Integer>>>,
                Tuple2<String,String>,                                                          //K
                Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>>() {            //V
            @Override
            public Iterator<Tuple2<Tuple2<String, String>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>> call(Tuple2<String, Iterable<Tuple3<String, Double, Integer>>> s) throws Exception {
                String user = s._1;
                //            movie, rating, numberOfRatingsOfthisUser
                Iterable<Tuple3<String, Double, Integer>> movies = s._2;
                List<Tuple3<String, Double, Integer>> listOfMovies = toList(movies);
                //Combination.findSortedCombinations(a, 2);


                return null;
            }
        });

        jsc.stop();
    }
    
    static List<Tuple3<String, Double, Integer>> toList(Iterable<Tuple3<String, Double, Integer>> iter) {
        List<Tuple3<String, Double, Integer>> list = new ArrayList<>();
        for (Tuple3<String, Double, Integer> t: iter) {
            list.add(t);
        }
        return list;
    }
}
