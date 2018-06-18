package demo.spark.mllib;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;
import scala.reflect.ClassTag$;

public class GmmEM {

  private static final String SPAM_FLAG_LONG_OPTION = "labelIdxTrain";
  private static final String SPAM_FLAG_OPTION = "w";
  private static final String SPAM_FLAG_OPTION_DESC =
      "label info column index of train file. default is -1";

  private static final String PRODUCT_CBLOF2HIVE_OPTION = "e";
  private static final String PRODUCT_CBLOF2HIVE_LONG_OPTION = "table";
  private static final String PRODUCT_CBLOF2HIVE_OPTION_DESC =
      "hive table name to store small rate. default is ''";

  private static final String TRAIN_OPTION = "i";
  private static final String TRAIN_LONG_OPTION = "train";
  private static final String TRAIN_OPTION_DESC =
      "input train file path property or hive source. default is ''";

  private static final String TEST_OPTION = "t";
  private static final String TEST_LONG_OPTION = "test";
  private static final String TEST_OPTION_DESC =
      "input test file path property. default equals intput train file ";

  private static final String OUTPUT_OPTION = "o";
  private static final String OUTPUT_LONG_OPTION = "output";
  private static final String OUTPUT_OPTION_DESC = "result output path property. default is ''";

  private static final String ABNORMAL_OPTION = "p";
  private static final String ABNORMAL_LONG_OPTION = "abnormal";
  private static final String ABNORMAL_OPTION_DESC =
      "abnormal result output path property. default is ''";

  private static final String ALPHA_OPTION = "a";
  private static final String ALPHA_LONG_OPTION = "alpha";
  private static final String ALPHA_OPTION_DESC =
      "the proportion of the large cluster. default is 0.9";

  private static final String BETA_OPTION = "b";
  private static final String BETA_LONG_OPTION = "beta";
  private static final String BETA_OPTION_DESC =
      "the distance of large and small cluster. default is 5";
  private static final String PRODUCT_OPTION = "f";
  private static final String PRODUCT_LONG_OPTION = "flag";
  private static final String PRODUCT_OPTION_DESC = "product name : sf, taxi, kc";

  private static final String K_OPTION = "k";
  private static final String K_LONG_OPTION = "k";
  private static final String K_OPTION_DESC = "k is a optional clusters num property.";

  private static final String NUM_ITERATION_OPTION = "n";
  private static final String NUM_ITERATION_LONG_OPTION = "numIterations";
  private static final String NUM_ITERATION_OPTION_DESC =
      "n is a optional num of iteration property. default is 10";

  private static final String SEPARATOR_OPTION = "s";
  private static final String SEPARATOR_LONG_OPTION = "separator";
  private static final String SEPARATOR_OPTION_DESC =
      "separator is a optional property. default is \\t";

  private static final String HELP_OPTION = "h";
  private static final String HELP_LONG_OPTION = "help";
  private static final String HELP_OPTION_DESC = "get help infomation";

  private static final String DAY_OPTION = "d";
  private static final String DAY_LONG_OPTION = "day";
  private static final String DAY_OPTION_DESC = "hive data day time, dataformat: yyyyMMdd";

  private static final String EXTRAIDXTRAIN_OPTION = "u";
  private static final String EXTRAIDXTRAIN_LONG_OPTION = "extraIdxTrain";
  private static final String EXTRAIDXTRAIN_OPTION_DESC =
      "extra info column index of train file. default is null";

  private static final String DATAIDXTRAIN_OPTION = "v";
  private static final String DATAIDXTRAIN_LONG_OPTION = "dataIdxTrain";
  private static final String DATAIDXTRAIN_OPTION_DESC =
      "data info column index of train file. default is null";

  private static final String EXTRAIDXTEST_OPTION = "x";
  private static final String EXTRAIDXTEST_LONG_OPTION = "extraIdxTest";
  private static final String EXTRAIDXTEST_OPTION_DESC =
      "extra info column index of test file. default is null";

  private static final String DATAIDXTEST_OPTION = "y";
  private static final String DATAIDXTEST_LONG_OPTION = "dataIdxTest";
  private static final String DATAIDXTEST_OPTION_DESC =
      "data info column index of test file. default is null";

  private static final String LABELIDXTEST_OPTION = "z";
  private static final String LABELIDXTEST_LONG_OPTION = "labelIdxTest";
  private static final String LABELIDXTEST_OPTION_DESC =
      "label info column index of test file. default is -1";

  private static final boolean HAS_OPTION_ARG = true;
  private static final boolean HAS_NO_OPTION_ARG = false;

  private static final int FILE_PARTITION_NUM = 100;
  
  private static CommandLine command;

  /**
   * 参数说明
   * 
   * (1) -i, --train 输入训练集 [必选项] (2) -u, --extraIdxTrain 训练数据的附加信息索引 0,1,3 (3) -v, --dataIdxTrain
   * 训练数据的参与聚类数据的索引 0,1,3 [必选项] (4) -w, --labelIdxTrain 训练数据的class信息索引 0,1,3 (5) -t, --test 输入测试集
   * [可选项] (6) -x, --extraIdxTest 测试数据的附加信息索引 0,1,3 (7) -y, --dataIdxTest 测试数据的参与聚类数据的索引
   * 0,1,3,如果extraIdxTest存在，则必须存在 (8) -z, --labelIdxTest 测试数据的class信息索引 0,1,3 (9) -o, --output
   * 输入结果保存hdfs路径，[可选项] (10) -k, --k 指定聚类个数，默认-1，按loglikelihood自动选定聚类个数，[可选项] (11) -s, --separator
   * 指定数据切分字符，默认 \t [可选项] (12) -n, --numIterations 指定迭代次数，默认10次 [可选项] (13) -a, --alpha 指定大聚类所占的比例，默认
   * 0.9 [可选项] (14) -b, --beta 划分大小聚类的距离，默认 5 [可选项] (15) -h, --help 打印帮助信息 [可选项] (16) -p, --abnormal
   * 保存异常类数据 [可选项] (17) -d, --day 查询hive数据日期，默认昨天 格式YYYYMMdd(18) -e 存储异常率的hive表名 (19) -f --flag
   * 产品线名称，包括 sf 、taxi、 kc
   * 
   */
  public static Options getOptions() {

    Options options = new Options();
    // 短参数 长参数 是否需要跟参数 解释说明
    options.addOption(TRAIN_OPTION, TRAIN_LONG_OPTION, HAS_OPTION_ARG, TRAIN_OPTION_DESC);
    options.addOption(TEST_OPTION, TEST_LONG_OPTION, HAS_OPTION_ARG, TEST_OPTION_DESC);
    options.addOption(OUTPUT_OPTION, OUTPUT_LONG_OPTION, HAS_OPTION_ARG, OUTPUT_OPTION_DESC);
    options.addOption(ABNORMAL_OPTION, ABNORMAL_LONG_OPTION, HAS_OPTION_ARG, ABNORMAL_OPTION_DESC);
    options.addOption(ALPHA_OPTION, ALPHA_LONG_OPTION, HAS_OPTION_ARG, ALPHA_OPTION_DESC);
    options.addOption(BETA_OPTION, BETA_LONG_OPTION, HAS_OPTION_ARG, BETA_OPTION_DESC);
    options.addOption(PRODUCT_CBLOF2HIVE_OPTION, PRODUCT_CBLOF2HIVE_LONG_OPTION, HAS_OPTION_ARG,
        PRODUCT_CBLOF2HIVE_OPTION_DESC);
    options.addOption(PRODUCT_OPTION, PRODUCT_LONG_OPTION, HAS_OPTION_ARG, PRODUCT_OPTION_DESC);
    options.addOption(K_OPTION, K_LONG_OPTION, HAS_OPTION_ARG, K_OPTION_DESC);
    options.addOption(NUM_ITERATION_OPTION, NUM_ITERATION_LONG_OPTION, HAS_OPTION_ARG, NUM_ITERATION_OPTION_DESC);
    options.addOption(SEPARATOR_OPTION, SEPARATOR_LONG_OPTION, HAS_OPTION_ARG,
        SEPARATOR_OPTION_DESC);
    options.addOption(HELP_OPTION, HELP_LONG_OPTION, HAS_NO_OPTION_ARG, HELP_OPTION_DESC);
    options.addOption(DAY_OPTION, DAY_LONG_OPTION, HAS_OPTION_ARG, DAY_OPTION_DESC);
    options.addOption(EXTRAIDXTRAIN_OPTION, EXTRAIDXTRAIN_LONG_OPTION, HAS_OPTION_ARG,
        EXTRAIDXTRAIN_OPTION_DESC);
    options.addOption(DATAIDXTRAIN_OPTION, DATAIDXTRAIN_LONG_OPTION, HAS_OPTION_ARG,
        DATAIDXTRAIN_OPTION_DESC);
    options.addOption(SPAM_FLAG_OPTION, SPAM_FLAG_LONG_OPTION, HAS_OPTION_ARG,
        SPAM_FLAG_OPTION_DESC);
    options.addOption(EXTRAIDXTEST_OPTION, EXTRAIDXTEST_LONG_OPTION, HAS_OPTION_ARG,
        EXTRAIDXTEST_OPTION_DESC);
    options.addOption(DATAIDXTEST_OPTION, DATAIDXTEST_LONG_OPTION, HAS_OPTION_ARG,
        DATAIDXTEST_OPTION_DESC);
    options.addOption(LABELIDXTEST_OPTION, LABELIDXTEST_LONG_OPTION, HAS_OPTION_ARG,
        LABELIDXTEST_OPTION_DESC);

    return options;

  }

  public static CommandLine getCommand(Options options, String[] args) throws ParseException {
    CommandLineParser commandParse = new BasicParser(); // 风格 ： 参数 参数值
    CommandLine commandLine = commandParse.parse(options, args);
    return commandLine;
  }

  public static void PrintHelp(Options options) {
    // print usage
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("GmmEM", options);
    System.exit(0);
  }

  public static void main(String[] args) throws ParseException {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
    SparkConf conf = new SparkConf();
    conf.setAppName("Gaussian Mixture Model EM");// .setMaster("local[4]");
    JavaSparkContext ctx = new JavaSparkContext(conf);
    gmmRun(args, ctx);
    ctx.stop();
  }

  public static JavaRDD<Instance> getDataWithRule(JavaSparkContext sc, String inputFilePath,
      String separator, String extraIdx, String dataIdx, int labelIdx) {
    List<Integer> dataIdxArr = MlUtil.parseCommaStringToArray(dataIdx);
    List<Integer> extraIdxArr = MlUtil.parseCommaStringToArray(extraIdx);
    return InputFormat.inputFormat(sc, inputFilePath, separator, extraIdxArr, dataIdxArr, labelIdx);
  }

  public static JavaRDD<Instance> getDataWithRule(JavaSparkContext sc, String tableName,
      String time, String extraIdx, String dataIdx, String labelIdx) {
    List<String> dataIdxArr = MlUtil.parseCommaStringToArrayStr(dataIdx);
    List<String> extraIdxArr = MlUtil.parseCommaStringToArrayStr(extraIdx);
    return InputFormat.inputFormat(sc, tableName, time, extraIdxArr, dataIdxArr, labelIdx);
  }

  public static void gmmRun(String[] args, JavaSparkContext sc) throws ParseException {
    Options options = getOptions();
    command = getCommand(options, args);
    // 默认参数
    String inputTrainFile = "";// 训练集文件路径，带label
    String inputTestFile = "";// 测试集文件路径，不带label
    String output = "";
    String outputAbnormal = "";
    String eTable = ""; //test.antispam_cblof
    String flag ="";
    int numIterations = 10;
    int k = -1;
    String separatorArg = "\t"; // 数据切分符
    double alpha = 0.9;
    int beta = 5;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    Date today = new Date(); // today
    int dayInterval = 60 * 60 * 24 * 1000;
    int delay = 1;
    String time = dateFormat.format(today.getTime() - delay * dayInterval); // 昨天

    String extraIdxTrain = ""; // 附加信息索引 0,1,3
    String dataIdxTrain = ""; // 用于计算聚类数据的索引 1,2,3
    String labelIdxTrain = "-1"; // class的索引

    String extraIdxTest = ""; // 附加信息索引 0,1,3
    String dataIdxTest = ""; // 用于计算聚类数据的索引 1,2,3
    int labelIdxTest = -1; // class的索引

    // 解析参数
    if (command.hasOption(HELP_OPTION)) {
      PrintHelp(options);
    }
    if (command.hasOption(TRAIN_OPTION)) {
      inputTrainFile = command.getOptionValue(TRAIN_OPTION);
    } else {
      System.err.println("-i 请输入输入文件或者hive表");
      PrintHelp(options);
    }
    if (command.hasOption(TEST_OPTION)) {
      inputTestFile = command.getOptionValue(TEST_OPTION);
    }
    if (command.hasOption(OUTPUT_OPTION)) {
      output = command.getOptionValue(OUTPUT_OPTION);
    }
    if (command.hasOption(ABNORMAL_OPTION)) {
      outputAbnormal = command.getOptionValue(ABNORMAL_OPTION);
    }
    if (command.hasOption(NUM_ITERATION_OPTION)) {
      try {
        numIterations = Integer.parseInt(command.getOptionValue(NUM_ITERATION_OPTION));
        if (numIterations <= 0) {
          System.err.println("-n 迭代次数不能小于1");
          PrintHelp(options);
        }
      } catch (Exception e) {
        System.err.println("-n 迭代次数必须为整数");
        PrintHelp(options);
      }

    }
    if (command.hasOption(SEPARATOR_OPTION)) {
      separatorArg = command.getOptionValue(SEPARATOR_OPTION);
    }
    if (command.hasOption(ALPHA_OPTION)) {
      alpha = Double.parseDouble(command.getOptionValue(ALPHA_OPTION));
    }
    if (command.hasOption(BETA_OPTION)) {
      beta = Integer.parseInt(command.getOptionValue(BETA_OPTION));
    }
    if (command.hasOption(PRODUCT_CBLOF2HIVE_OPTION)) {
      eTable = command.getOptionValue(PRODUCT_CBLOF2HIVE_OPTION);
    }
    if (command.hasOption(K_OPTION)) {
      try {
        k = Integer.parseInt(command.getOptionValue(K_OPTION));
      } catch (Exception e) {
        System.err.println("-k 聚类个数必须为整数");
        PrintHelp(options);
      }
    }
    if (command.hasOption(DAY_OPTION)) {
      time = command.getOptionValue(DAY_OPTION);
    }
    if (command.hasOption(PRODUCT_OPTION)) {
      flag = command.getOptionValue(PRODUCT_OPTION);
    }

    if (command.hasOption(EXTRAIDXTRAIN_OPTION)) {
      extraIdxTrain = command.getOptionValue(EXTRAIDXTRAIN_OPTION);
    }
    if (command.hasOption(DATAIDXTRAIN_OPTION)) {
      dataIdxTrain = command.getOptionValue(DATAIDXTRAIN_OPTION);
    } else {
      System.err.println("-v 请输入输入文件的参与聚类数据下标，例如 0,1,4,3");
      PrintHelp(options);
    }
    if (command.hasOption(SPAM_FLAG_OPTION)) {
      labelIdxTrain = command.getOptionValue(SPAM_FLAG_OPTION);
    }
    if (command.hasOption(EXTRAIDXTEST_OPTION)) {
      extraIdxTest = command.getOptionValue(EXTRAIDXTEST_OPTION);
    }
    if (command.hasOption(DATAIDXTEST_OPTION)) {
      dataIdxTest = command.getOptionValue(DATAIDXTEST_OPTION);
    } else if (!"".equals(inputTestFile)) {
      System.err.println("-y 请输入训练文件的参与聚类数据下标，例如 0,1,4,3");
      PrintHelp(options);

    }
    if (command.hasOption(LABELIDXTEST_OPTION)) {
      labelIdxTest = Integer.parseInt(command.getOptionValue(LABELIDXTEST_OPTION));
    }

    final String separator = separatorArg; // 形参方法参数必须是final
    // 训练数据
    JavaRDD<Instance> train = null;
    if (MlUtil.isInteger(dataIdxTrain.split(",")[0])) { // not search hive
      train =
          getDataWithRule(sc, inputTrainFile, separator, extraIdxTrain, dataIdxTrain,
              Integer.parseInt(labelIdxTrain));
    } else {
      train = getDataWithRule(sc, inputTrainFile, time, extraIdxTrain, dataIdxTrain, labelIdxTrain);
    }
    train.persist(StorageLevel.MEMORY_AND_DISK());
    // estimate the number of clusters by cross validation on the training
    if (k == -1) {
      k = CVClusters(train, numIterations, sc, alpha, beta);
    }
    JavaRDD<Vector> trainData = train.map(e -> e.getData()).persist(StorageLevel.MEMORY_AND_DISK());
    
    GaussianMixtureModel clusters = MlUtil.getGaussianMixtureModel(trainData, k, numIterations);

    JavaPairRDD<Tuple2<Instance, Integer>, Double> result = null;
    if (!"".equals(inputTestFile)) { // 指定测试数据
      // 测试数据
      JavaRDD<Instance> test =
          getDataWithRule(sc, inputTestFile, separator, extraIdxTest, dataIdxTest, labelIdxTest);
      result = run(clusters, test, numIterations, k, sc, alpha, beta, outputAbnormal, time, eTable, flag);

      result.persist(StorageLevel.MEMORY_AND_DISK());
      if (!"".equals(output)) {
        result.repartition(FILE_PARTITION_NUM).saveAsTextFile(output); // save to hdfs
      }

      // 计算auc
      double auc = MlUtil.getAuc(clusters, trainData, train, alpha, beta, "", FILE_PARTITION_NUM);
      System.err.println("auc = " + auc);

    } else { // 未指定测试数据
      result =
          run(clusters, train, numIterations, k, sc, alpha, beta, outputAbnormal, time, eTable, flag);
      result.persist(StorageLevel.MEMORY_AND_DISK());
      if (!"".equals(output)) {
        result.repartition(FILE_PARTITION_NUM).saveAsTextFile(output); // save to hdfs
      }
      if (command.hasOption(SPAM_FLAG_OPTION)) {
        // 计算auc
        double auc = MlUtil.getAuc(result, alpha, beta, "");
        System.err.println("auc = " + auc);
      }

    }

  }

  public static int CVClusters(JavaRDD<Instance> data, int maxIterations, JavaSparkContext sc,
      double alpha, int beta) {

    int numInstances = (int) data.count();
    int numFolds = 0;
    if (numInstances < 10) {
      numFolds = numInstances;
    } else {
      numFolds = 10;
    }
    double errorSum = -Double.MAX_VALUE;
    boolean continueFlag = true;
    int k = 1;
    while (continueFlag) {
      double templl = 0d;

      Tuple2<RDD<Instance>, RDD<Instance>>[] kFoldResult =
          MLUtils.kFold(data.rdd(), numFolds, 1, ClassTag$.MODULE$.apply(Instance.class));

      for (Tuple2<RDD<Instance>, RDD<Instance>> kFold : kFoldResult) {
        double ttl = doEM(kFold._1.toJavaRDD(), k, maxIterations, sc);
        templl += ttl;
      }

      System.err.println("CVLogLikely:" + templl + ", k=" + k);
      if (errorSum < templl) {
        k += 1;
        errorSum = templl;

      } else {
        continueFlag = false;
      }
    }
    k -= 1;
    return k;
  }

  private static double doEM(JavaRDD<Instance> cvTrain, int k, int maxIterations,
      JavaSparkContext sc) {
    JavaRDD<Vector> cvTrainData =
        cvTrain.map(e -> e.getData()).persist(StorageLevel.MEMORY_AND_DISK());

    GaussianMixtureModel clusters = MlUtil.getGaussianMixtureModel(cvTrainData, k, maxIterations);
    cvTrainData.unpersist();
    return MlUtil.E(cvTrainData, clusters, k, maxIterations, sc);
  }

  private static JavaPairRDD<Tuple2<Instance, Integer>, Double> run(GaussianMixtureModel clusters,
      JavaRDD<Instance> test, int maxIterations, int k, JavaSparkContext sc, double alpha,
      int beta, String outputAbnormal, String time, String eTable, String flag) {

    JavaRDD<Integer> prediction = clusters.predict(test.map(e -> e.getData()));
    JavaPairRDD<Instance, Integer> testDataWithPrediction =
        test.zip(prediction).persist(StorageLevel.MEMORY_AND_DISK());
    // //打印精确度
    MlUtil.outputStatistics(testDataWithPrediction, k, command.hasOption(SPAM_FLAG_OPTION));
    // //计算异常值
    Tuple2<JavaPairRDD<Tuple2<Instance, Integer>, Double>, Double> cblof =
        MlUtil.cblof(testDataWithPrediction, alpha, beta, outputAbnormal, FILE_PARTITION_NUM);
    JavaPairRDD<Tuple2<Instance, Integer>, Double> resultData = cblof._1;
    double smallRate = cblof._2;
    if (!"local".equalsIgnoreCase(sc.master().substring(0, 5)) && !"".equals(eTable) && !"".equals(flag)) {
      MlUtil.store2Hive(time, String.valueOf(smallRate), eTable, flag, sc);
    }
    testDataWithPrediction.unpersist();
    return resultData;
  }



}
