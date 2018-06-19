package demo.spark.mllib;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.clustering.ExpectationSum;
import org.apache.spark.mllib.clustering.GaussianMixture;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class MlUtil {
  // 归一化处理
  public static JavaRDD<Vector> normalization(JavaRDD<Vector> data) {

    RowMatrix rowMatrix = new RowMatrix(data.rdd());
    MultivariateStatisticalSummary statistics = rowMatrix.computeColumnSummaryStatistics();
    double[] min = statistics.min().toArray();
    double[] max = statistics.max().toArray();
    double[] max_min = new double[max.length];
    for (int i = 0; i < max.length; i++) {
      max_min[i] = max[i] - min[i];

    }
    JavaRDD<Vector> rowMatrixResult = data.map(new Function<Vector, Vector>() {
      @Override
      public Vector call(Vector vec) throws Exception {
        double[] array = vec.toArray();
        double[] result = new double[array.length];
        for (int i = 0; i < array.length; i++) {
          if (max_min[i] != 0d) {
            result[i] = (array[i] - min[i]) / (max_min[i]);
          } else {
            result[i] = 0.0d;
          }

        }

        return Vectors.dense(result);
      }
    });
    return rowMatrixResult;

  }

  // 得到高斯混合模型
  public static GaussianMixtureModel getGaussianMixtureModel(JavaRDD<Vector> data, int k,
      int maxIterations) {
    GaussianMixtureModel clusters =
        new GaussianMixture().setK(k).setSeed(997).setMaxIterations(maxIterations).run(data);
    return clusters;
  }

  public static List<Integer> parseCommaStringToArray(String str) {
      String[] stringSplit = str.split(",");
      List<Integer> arr = new ArrayList<Integer>();
      if("".equals(str)) {
        return arr;
      }
      for (String c : stringSplit) {
        arr.add(Integer.parseInt(c));
      }
      return arr;
  }
  
  public static List<String> parseCommaStringToArrayStr(String str) {
    String[] stringSplit = str.split(",");
    List<String> arr = new ArrayList<String>();
    if("".equals(str)) {
      return arr;
    }
    for (String c : stringSplit) {
      arr.add(c);
    }
    return arr;
  }

  /**
   * Finds the minimum error mapping of classes to clusters. Recursively considers all possible
   * class to cluster assignments.
   * 
   * @param numClusters the number of clusters
   * @param lev the cluster being processed
   * @param counts the counts of classes in clusters
   * @param clusterTotals the total number of examples in each cluster
   * @param current the current path through the class to cluster assignment tree
   * @param best the best assignment path seen
   * @param error accumulates the error for a particular path
   */
  public static void mapClasses(int numClusters, int lev, int[][] counts,
      List<Integer> clusterTotals, List<Double> current, List<Double> best, int error) {
    // leaf
    if (lev == numClusters) {
      if (error < best.get(numClusters)) {
        best.set(numClusters, Double.parseDouble("" + error));
        for (int i = 0; i < numClusters; i++) {
          best.set(i, current.get(i));
        }
      }
    } else {
      // empty cluster -- ignore
      if (clusterTotals.get(lev) == 0) {
        current.set(lev, -1d); // cluster ignored
        mapClasses(numClusters, lev + 1, counts, clusterTotals, current, best, error);
      } else {
        // first try no class assignment to this cluster
        current.set(lev, -1d); // cluster assigned no class (ie all
                               // errors)
        mapClasses(numClusters, lev + 1, counts, clusterTotals, current, best,
            error + clusterTotals.get(lev));
        // now loop through the classes in this cluster
        for (int i = 0; i < counts[0].length; i++) {
          if (counts[lev][i] > 0) {
            boolean ok = true;
            // check to see if this class has already been assigned
            for (int j = 0; j < lev && ok; j++) {
              if (current.get(j).intValue() == i) {
                ok = false;
              }
            }
            if (ok) {
              current.set(lev, Double.valueOf("" + i));
              mapClasses(numClusters, lev + 1, counts, clusterTotals, current, best,
                  (error + (clusterTotals.get(lev) - counts[lev][i])));
            }
          }
        }
      }
    }

  } // function mapClasses

  public static double E(JavaRDD<Vector> data, GaussianMixtureModel clusters, int k,
      int maxIterations, JavaSparkContext sc) {

//    int d = data.first().size();
//    Function2<ExpectationSum, breeze.linalg.Vector<Object>, ExpectationSum> addExpectationSum =
//        new Function2<ExpectationSum, breeze.linalg.Vector<Object>, ExpectationSum>() {
//          @Override
//          public ExpectationSum call(ExpectationSum v1, breeze.linalg.Vector<Object> v2)
//              throws Exception {
//
//            return ExpectationSum.add(clusters.weights(), clusters.gaussians(), v1, v2);
//          }
//        };
//    Function2<ExpectationSum, ExpectationSum, ExpectationSum> combine =
//        new Function2<ExpectationSum, ExpectationSum, ExpectationSum>() {
//          @Override
//          public ExpectationSum call(ExpectationSum v1, ExpectationSum v2) throws Exception {
//            return v1.$plus$eq(v2);
//          }
//        };
//    ExpectationSum sums = data.map(new Function<Vector, breeze.linalg.Vector<Object>>() {
//      @Override
//      public breeze.linalg.Vector<Object> call(Vector v1) throws Exception {
//        return v1.toBreeze();
//        // return v1._2.asBreeze();//spark-core-2.0.0
//      }
//    }).aggregate(ExpectationSum.zero(k, d), addExpectationSum, combine);//
//    return sums.logLikelihood();// this is the freshly computed
//    // log-likelihood

      return 0.0d; ///这个与代码无关，仅仅是为了保持无措
  }

  /**
   * 统计聚类准确信息
   * 
   * @param testDataWithPrediction
   * @param numClusters
   * @param hasSpamFlag
   * @return
   */
  public static void outputStatistics(JavaPairRDD<Instance, Integer> testDataWithPrediction,
      int numClusters, boolean hasSpamFlag) {
    JavaPairRDD<Integer, Integer> sampleKV = testDataWithPrediction
        .mapToPair(new PairFunction<Tuple2<Instance, Integer>, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Tuple2<Instance, Integer> t) throws Exception {

            return new Tuple2<Integer, Integer>(t._2, 1);
          }
        })
        .reduceByKey(new Function2<Integer, Integer, Integer>() {


          @Override
          public Integer call(Integer i1, Integer i2) throws Exception {

            return i1 + i2;
          }
        });
    
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("\n\n");
    stringBuffer.append("Predict Instances\n\n");
    long sumAll = testDataWithPrediction.count();
    for (Tuple2<Integer, Integer> line : sampleKV.collect()) {
      stringBuffer.append(
          line._1 + "\t\t" + line._2 + "( " + Double.valueOf(line._2) / sumAll * 100 + "%)\n");
    }
    stringBuffer.append("\n\n");
    System.err.println(stringBuffer.toString());
    
    if (hasSpamFlag) {
      JavaRDD<Tuple2<Object, Object>> labelAndpredictions = testDataWithPrediction
          .map(new Function<Tuple2<Instance, Integer>, Tuple2<Object, Object>>() {
            public Tuple2<Object, Object> call(Tuple2<Instance, Integer> line) {
              return new Tuple2<Object, Object>(parseDouble(line._1.getLabel()), (double) line._2);
            }
          });

      MulticlassMetrics metrics = new MulticlassMetrics(labelAndpredictions.rdd());
      System.err.println("Confusion matrix: \n" + metrics.confusionMatrix());
    }
  }

  // 计算异常值
  public static Tuple2<JavaPairRDD<Tuple2<Instance, Integer>, Double>, Double> cblof(
      JavaPairRDD<Instance, Integer> testDataWithPrediction, double alpha, int beta,
      String outputAbnormal, int filePartitionNum) {

    long dataNum = testDataWithPrediction.count();
    double bigClusterSize = dataNum * alpha;
    List<Tuple2<Integer, Integer>> labelNumSort = testDataWithPrediction
        .mapToPair(new PairFunction<Tuple2<Instance, Integer>, Integer, Integer>() {

          @Override
          public Tuple2<Integer, Integer> call(Tuple2<Instance, Integer> t) throws Exception {
            return new Tuple2<Integer, Integer>(t._2, 1);
          }

        })
        .reduceByKey(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer i1, Integer i2) throws Exception {
            return i1 + i2;
          }
        }).mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t) throws Exception {
            return new Tuple2<Integer, Integer>(t._2, t._1);
          }
        }).sortByKey(false)
        .mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t) throws Exception {
            return new Tuple2<Integer, Integer>(t._2, t._1);
          }
        }).collect();

    List<Integer> bigClusterIndex = new ArrayList<Integer>(); // 收集聚类标签
    List<Integer> smallClusterIndex = new ArrayList<Integer>();// 收集聚类标签
    // 按照alpha和beta规则切分，赋值到bigClusterIndex和smallClusterIndex
    splitDataWithRule(labelNumSort, bigClusterIndex, smallClusterIndex, bigClusterSize, beta);

    Map<Integer, Vector> bigClusterCenters = new HashMap<Integer, Vector>(); // 各个大聚类的中心点map
    Map<Integer, Vector> smallClusterCenters = new HashMap<Integer, Vector>();// 各个小聚类的中心点map，暂时没用到

    for (Integer x : bigClusterIndex) {
      bigClusterCenters.put(x, getClusterCenter(testDataWithPrediction, x));

    }
    for (Integer x : smallClusterIndex) {
      smallClusterCenters.put(x, getClusterCenter(testDataWithPrediction, x));

    }

    // 计算cblof value值
    // 计算大聚类的cblof value值

    boolean first = true;
    JavaPairRDD<Tuple2<Instance, Integer>, Double> bigCblofValue = null;
    for (Integer predictLabel : bigClusterIndex) {

      JavaPairRDD<Instance, Integer> predictRdd =
          testDataWithPrediction.filter(e -> (e._2 == predictLabel.intValue()));
      if (predictRdd.count() != 0) {
        JavaRDD<Vector> data = predictRdd.map(e -> e._1.getData());

        Vector center = bigClusterCenters.get(predictLabel);
        JavaRDD<Vector> normalData = normalization(data);
        JavaRDD<Double> cblofvalue = normalData.map(vec -> Vectors.sqdist(vec, center)); // 只算距离

        if (first) {
          bigCblofValue = predictRdd.zip(cblofvalue);
          first = false;
        } else {
          bigCblofValue = bigCblofValue.union(predictRdd.zip(cblofvalue));
        }
      }
    }

    // //计算小聚类的cblof value值
    JavaPairRDD<Tuple2<Instance, Integer>, Double> smallCblofValue = null;
    first = true;
    for (Integer predictLabel : smallClusterIndex) {
      JavaPairRDD<Instance, Integer> predictRdd =
          testDataWithPrediction.filter(e -> (e._2 == predictLabel.intValue()));
      if (predictRdd.count() != 0) {
        JavaRDD<Vector> data = predictRdd.map(e -> e._1.getData());

        JavaRDD<Vector> normalData = normalization(data);
        JavaRDD<Double> cblofvalue = normalData.map(new Function<Vector, Double>() {
          @Override
          public Double call(Vector vec) throws Exception {
            double smallDis = Double.MAX_VALUE;
            Set<Integer> keySet = bigClusterCenters.keySet();
            for (Integer key : keySet) {
              double dis = Vectors.sqdist(vec, bigClusterCenters.get(key));
              if (smallDis > dis) {
                smallDis = dis;
              }
            }
            // return smallDis * dataCount;
            return smallDis;
          }
        });

        if (first) {
          smallCblofValue = predictRdd.zip(cblofvalue);
          first = false;
        } else {
          smallCblofValue = smallCblofValue.union(predictRdd.zip(cblofvalue));
        }

      }
    }
    double smallRateResult = 0.0d;
    if (smallCblofValue != null && !smallCblofValue.isEmpty()) {
      smallCblofValue.persist(StorageLevel.MEMORY_AND_DISK());

      long smallCount = smallCblofValue.count();

      bigCblofValue = bigCblofValue.union(smallCblofValue); // 大小 合并
      bigCblofValue.persist(StorageLevel.MEMORY_AND_DISK());
      long bigCount = bigCblofValue.count();
      final double smallRate = smallCount / Double.parseDouble("" + bigCount);
      smallRateResult = smallRate;
      System.err.println("small cluster rate = " + smallRate * 100 + "%");
      if (!"".equals(outputAbnormal)) {
        smallCblofValue.repartition(filePartitionNum).saveAsTextFile(outputAbnormal);
        smallCblofValue.unpersist();
      }

    } else {
      System.err.println("small cluster rate = 0.0%");
    }

    return new Tuple2(bigCblofValue,smallRateResult);

  }

  public static Vector getClusterCenterByKmeans(JavaPairRDD<Instance, Integer> predictionWithLabel,
      int predictLabel) {
    JavaRDD<Vector> data =
        predictionWithLabel.filter(e -> e._2 == predictLabel).map(e -> e._1.getData());
    JavaRDD<Vector> normalData = normalization(data);
    KMeansModel model = KMeans.train(normalData.rdd(), 1, 10);
    Vector[] centers = model.clusterCenters();
    return centers[0];
  }

  public static Vector getClusterCenter(JavaPairRDD<Instance, Integer> predictionWithLabel,
      int predictLabel) {
    JavaRDD<Vector> data =
        predictionWithLabel.filter(e -> e._2 == predictLabel).map(e -> e._1.getData());
    JavaRDD<Vector> normalData = normalization(data);
    RowMatrix rowMatrix = new RowMatrix(normalData.rdd());
    MultivariateStatisticalSummary statistics = rowMatrix.computeColumnSummaryStatistics();
    return statistics.mean();
  }

  // 按照alpha和beta规则切分
  public static void splitDataWithRule(List<Tuple2<Integer, Integer>> labelNumSort,
      List<Integer> bigClusterIndex, List<Integer> smallClusterIndex, double bigClusterSize,
      int beta) {
    boolean found = false;
    int i = 0;
    int sum = 0; // 累计和
    while (i < labelNumSort.size() - 1) {
      sum += labelNumSort.get(i)._2;
      if (found) {
        smallClusterIndex.add(labelNumSort.get(i)._1);
      } else {
        bigClusterIndex.add(labelNumSort.get(i)._1);
      }
      if (found || (sum > bigClusterSize
          && (labelNumSort.get(i)._2 / labelNumSort.get(i + 1)._2) >= beta)) {
        found = true;
      }
      i += 1;
    }
    if (found) {
      smallClusterIndex.add(labelNumSort.get(i)._1);
    } else {
      bigClusterIndex.add(labelNumSort.get(i)._1);
    }
  }

  public static double calcAuc(JavaRDD<Tuple2<Object, Object>> distanceAndLabel) {
    BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(distanceAndLabel.rdd());
    return metrics.areaUnderROC();
  }

  public static double getAuc(GaussianMixtureModel clusters, JavaRDD<Vector> trainData,
      JavaRDD<Instance> train, double alpha, int beta, String outputAbnormal, int filePartitionNum) {
    JavaRDD<Integer> prediction = clusters.predict(trainData);
    JavaPairRDD<Instance, Integer> trainDataWithPrediction = train.zip(prediction).persist(StorageLevel.MEMORY_AND_DISK());
    JavaPairRDD<Tuple2<Instance, Integer>, Double> resul =
        cblof(trainDataWithPrediction, alpha, beta, outputAbnormal, filePartitionNum)._1;
    trainDataWithPrediction.unpersist();
    resul.persist(StorageLevel.MEMORY_AND_DISK());
   
      JavaRDD<Tuple2<Object, Object>> aucData = resul
          .map(new Function<Tuple2<Tuple2<Instance, Integer>, Double>, Tuple2<Object, Object>>() {

            @Override
            public Tuple2<Object, Object> call(Tuple2<Tuple2<Instance, Integer>, Double> t)
                throws Exception {
              return new Tuple2<Object, Object>(t._2, parseDouble(t._1._1.getLabel()));
            }
          });
      resul.unpersist();
      double auc = calcAuc(aucData);
      return auc;
    
  }

  public static double getAuc(JavaPairRDD<Tuple2<Instance, Integer>, Double> result, double alpha,
      int beta, String outputAbnormal) {
    JavaRDD<Tuple2<Object, Object>> aucData =
        result
            .map(new Function<Tuple2<Tuple2<Instance, Integer>, Double>, Tuple2<Object, Object>>() {

              @Override
              public Tuple2<Object, Object> call(Tuple2<Tuple2<Instance, Integer>, Double> t)
                  throws Exception {
                return new Tuple2<Object, Object>(t._2, parseDouble(t._1._1.getLabel()));
              }
            });
    double auc = calcAuc(aucData);
    return auc;
  }

  public static double parseDouble(String stringDouble) {
    if (stringDouble.isEmpty() || "NULL".equals(stringDouble)) {
      return 0;
    } else {
      return Double.parseDouble(stringDouble);
    }
  }

  public static boolean isInteger(String str) {
    Pattern pattern = Pattern.compile("^[-\\\\+]?[\\\\d]*$");
    return pattern.matcher(str.trim()).matches();
  }
  /**
   * 将 value 插入到名为tableName表的flag列 
   * @param time
   * @param value
   * @param tableName
   * @param flag
   * @param sc
   */
  public static void store2Hive(String time, String value, String tableName, String flag,
      JavaSparkContext sc) {

//    Cblof cblof = new Cblof(time);
//    HiveContext hiveContext = new HiveContext(sc.sc());
//
//    String sql = "select * from " + tableName + " where create_time = " + time + " limit 1";
//    Dataset result = hiveContext.sql(sql);
//
//
//    if (result.count() > 0) {
//      for (String col : result.columns()) {
//        cblof.setValue(col, result.first().getAs(col));
//      }
//    }
//    cblof.setValue(flag, value);
//    System.out.println(cblof);
//    List<Cblof> values = new ArrayList<>();
//    values.add(cblof);
//    JavaRDD<Row> valueRdd = sc.parallelize(values).map(new Function<Cblof, Row>() {
//      @Override
//      public Row call(Cblof v1) throws Exception {
//
//        return RowFactory.create(v1.getTime(), v1.getKc(), v1.getTaxi(), v1.getSf());
//      }
//    });
//    List<StructField> structFields = new ArrayList<StructField>();
//    structFields.add(DataTypes.createStructField("date", DataTypes.StringType, true));
//    structFields.add(DataTypes.createStructField("kc", DataTypes.StringType, true));
//    structFields.add(DataTypes.createStructField("taxi", DataTypes.StringType, true));
//    structFields.add(DataTypes.createStructField("sf", DataTypes.StringType, true));
//    StructType structType = DataTypes.createStructType(structFields);
//    // delete
//    deleteHiveInfo(time, tableName, hiveContext);
//    // store
//    hiveContext.createDataFrame(valueRdd, structType).insertInto(tableName);

  }

  /**
   * 删除名为tableName的hive表的时间为time的数据 
   * @param time
   * @param tableName
   * @param hiveContext
   */
  public static void deleteHiveInfo(String time, String tableName, HiveContext hiveContext) {
    String sql =
        "insert overwrite table " + tableName + " select * from " + tableName
            + " where create_time <>" + time;
    hiveContext.sql(sql);
  }


}


class Comp implements Comparator<Tuple2<String, Integer>>, Serializable {
  @Override
  public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
    if (!o1._1.equals(o2._1)) {
      return o1._1.compareTo(o2._1);
    }
    return o1._2 - o2._2;
  }
}

