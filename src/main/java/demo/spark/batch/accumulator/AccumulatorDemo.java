package demo.spark.batch.accumulator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;


public class AccumulatorDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        Accumulator<Integer> sum = jsc.accumulator(0);
        List<Integer> numberList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = jsc.parallelize(numberList);
        numbers.foreach(new VoidFunction<Integer>() {
            ////在函数内部，可以直接使用Accumulator变量，调用add()方法，累计值
            @Override
            public void call(Integer t) throws Exception {
                sum.add(t);
            }
        });

        //在driver端，调用Accumulator的value方法得到其值
        System.out.println(sum.value());
        jsc.stop();
    }
}
