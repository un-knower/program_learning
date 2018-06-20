package demo.spark.batch.customerorder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.math.Ordered;

import java.io.Serializable;
import java.util.List;

class SecondarySort implements Ordered<SecondarySort> ,Serializable {

    // 自定义二次排序的两个key
    private int first;
    private int second;

    // getter/setter
    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    public SecondarySort() {

    }
    public SecondarySort(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(SecondarySort that) {
        if (this.first - that.first != 0) {
            return this.first - that.first;
        } else {
            return this.second - that.second;
        }
    }

    @Override
    public boolean $less(SecondarySort that) {
        if (this.first < that.first) {
            return true;
        } else if (this.first == that.first && this.second < that.second) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $greater(SecondarySort that) {
        if (this.first > that.first) {
            return true;
        } else if (this.first == that.first && this.second > that.second) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $less$eq(SecondarySort that) {
        if (this.$less(that)) {
            return true;
        } else if (this.first == that.second && this.second == that.second) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean $greater$eq(SecondarySort that) {
        if (this.$greater(that)) {
            return true;
        } else if (this.first == that.second && this.second == that.second) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(SecondarySort that) {
        if (this.first - that.first != 0) {
            return this.first - that.first;
        } else {
            return this.second - that.second;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SecondarySort that = (SecondarySort)obj;
        if (first != that.first) {
            return false;
        }
        return second == that.second;

    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + first;
        result = prime * result + second;
        return result;
    }
}

// 二次排序，具体实现步骤
// 第一步：按照Ordered和Serrializable接口实现自定义排序的Key
// 第二步：将要进行二次排序的文件加载进来生成《key，value》类型的RDD
// 第三步：使用sortByKey基于自定义的Key进行二次排序
// 第四步：去除掉排序的key，，只保留排序结果
public class SecondarySortTest {
    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]").setAppName("secondary_sort");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = jsc.textFile("C:\\Users\\qingjian\\IdeaProjects\\program_learning\\src\\main\\java\\demo\\hadoop\\sort\\secondarysort");

        JavaPairRDD<SecondarySort, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySort, String>() {
            // SecondarySort key = new SecondarySort(); // 挪到这里就不对了

            @Override
            public Tuple2<SecondarySort, String> call(String line) throws Exception {
                String[] split = line.split(" ");
                SecondarySort key = new SecondarySort();  // 这一行挪到外面就不对了
                key.setFirst(Integer.valueOf(split[0]));
                key.setSecond(Integer.valueOf(split[1]));

                return new Tuple2<>(key, line);
            }
        });

        JavaPairRDD<SecondarySort, String> sorted = pairs.sortByKey(); // 完成二次排序
        // 过滤掉排序后自定的key，保留排序的结果
        JavaRDD<String> secondarySorted = sorted.map(new Function<Tuple2<SecondarySort, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySort, String> secondarySortStringTuple2) throws Exception {
                return secondarySortStringTuple2._2;
            }
        });

        // 下面注释代码是局部有序，这样foreach打印出来是局部有序
//        secondarySorted.foreach(new VoidFunction<String>() {
//            @Override
//            public void call(String s) throws Exception {
//                System.out.println(s);
//            }
//        });

        List<String> collect = secondarySorted.collect();
        for(String c:collect){
            System.out.println(c);
        }


    }



}
