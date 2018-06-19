package demo.spark.mllib;

import java.io.Serializable;

import org.apache.spark.mllib.linalg.Vector;

public class Instance implements Serializable {
  String extrea; // 附加信息
  Vector data; // 参与聚类数据
  String label; // 类别

  public Instance(String extrea, Vector data, String label) {
    this.extrea = extrea;
    this.data = data;
    this.label = label;
  }

  public String getExtrea() {
    return extrea;
  }

  public void setExtrea(String extrea) {
    this.extrea = extrea;
  }

  public Vector getData() {
    return data;
  }

  public void setData(Vector data) {
    this.data = data;
  }

  public String getLabel() {
    return label;
  }

  public void setLabel(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return extrea + "," + data + "," + label;
  }


}

