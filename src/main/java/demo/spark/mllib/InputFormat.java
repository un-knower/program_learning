package demo.spark.mllib;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;


public class InputFormat {
  public static JavaRDD<Instance> inputFormat(JavaSparkContext sc, String filePath,
      String separator, List<Integer> extraInfoBoundary, List<Integer> dataInfoBoundary,
      int labelInfoBoundary) {
    JavaRDD<String> con = sc.textFile(filePath);

    JavaRDD<Instance> data = con.map(new Function<String, Instance>() {
      @Override
      public Instance call(String line) throws Exception {
        String[] arr = line.split(separator, -1);
        StringBuffer extraInfo = new StringBuffer();
        List<Double> dataInfo = new ArrayList<>();
        // 加载附加信息
        for (Integer idx : extraInfoBoundary) {
          extraInfo.append(arr[idx] + ",");
        }
        if (extraInfo.length() > 0) {
          extraInfo.deleteCharAt(extraInfo.length() - 1);
        }
        // 加载聚类数据
        for (Integer idx : dataInfoBoundary) {
          dataInfo.add(MlUtil.parseDouble(arr[idx]));
        }

        double[] dataInfoArr = new double[dataInfo.size()];
        for (int i = 0; i < dataInfo.size(); i++) {
          dataInfoArr[i] = dataInfo.get(i);
        }
        // label信息
        String label = "";
        if (labelInfoBoundary >= 0) { // default -1
          label = arr[labelInfoBoundary];
        }
        return new Instance(extraInfo.toString(), Vectors.dense(dataInfoArr), label);
      }
    });

    return data;

  }

  public static JavaRDD<Instance> inputFormat(JavaSparkContext sc, String tableName, String time,
      List<String> extraIdxArr, List<String> dataIdxArr, String labelIdx) {
    // organize sql
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("select ");

    HashSet<String> selectSet = new HashSet<>();
    for (String select : extraIdxArr) {
      selectSet.add(select);
    }
    for (String select : dataIdxArr) {
      selectSet.add(select);
    }
    for (String select : selectSet) {
      stringBuffer.append(select + ",");
    }
    if (!"-1".equals(labelIdx)) {
      stringBuffer.append(labelIdx);
    } else {
      stringBuffer.deleteCharAt(stringBuffer.length() - 1);
    }

    String selectStr = stringBuffer.toString();
    stringBuffer.append(" from (").append(selectStr)
        .append(",row_number() over (partition by oid order by aegis_mtime desc)num");

    stringBuffer.append(" from ").append(tableName).append(" where concat(year,month,day)=")
        .append(time).append(") t where t.num=1");

    String sql = stringBuffer.toString();
    HiveContext hiveContext = new HiveContext(sc.sc());
    JavaRDD<Row> result = hiveContext.sql(sql).javaRDD();
    JavaRDD<Instance> data = result.map(new Function<Row, Instance>() {

      @Override
      public Instance call(Row v1) throws Exception {
        StringBuffer extraBufferr = new StringBuffer();
        double[] dataInfoArr = new double[dataIdxArr.size()];
        for (String field : extraIdxArr) {
          Object extra = v1.getAs(field);
          if (extra == null) {
            extraBufferr.append("0,");
          } else {
            extraBufferr.append(v1.getAs(field).toString()).append(",");
          }
        }
        if (extraBufferr.length() > 0) {
          extraBufferr.deleteCharAt(extraBufferr.length() - 1);
        }
        for (int i = 0; i < dataIdxArr.size(); i++) {
          Object field = v1.getAs(dataIdxArr.get(i));
          if (field != null) {
            dataInfoArr[i] = MlUtil.parseDouble(field.toString());

          } else {
            dataInfoArr[i] = 0;
          }
        }
        // label信息
        String label = "";
        if (!"-1".equals(labelIdx)) {
          Object field = v1.getAs(labelIdx);
          if (field != null) {
            label = field.toString();
          }
        }
        return new Instance(extraBufferr.toString(), Vectors.dense(dataInfoArr), label);
      }

    });

    return data;
  }
}

