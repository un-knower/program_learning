package demo.spark.mllib;

import java.io.Serializable;

public class Cblof implements Serializable {
  private String time = "null";
  private String kc = "null";
  private String taxi = "null";
  private String sf = "null";

  public Cblof(String time) {
    this.time = time;
  }

  public Cblof(String time, String kc, String taxi, String sf) {
    this.time = time;
    this.kc = kc;
    this.taxi = taxi;
    this.sf = sf;
  }

  public String getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = time;
  }

  public String getKc() {
    return kc;
  }

  public void setKc(String kc) {
    this.kc = kc;
  }

  public String getTaxi() {
    return taxi;
  }

  public void setTaxi(String taxi) {
    this.taxi = taxi;
  }

  public String getSf() {
    return sf;
  }

  public void setSf(String sf) {
    this.sf = sf;
  }

  public void setValue(String field, String value) {
    switch (field) {
      case "kc": {
        this.kc = value;
        break;
      }
      case "taxi": {
        this.taxi = value;
        break;
      }
      case "sf": {
        this.sf = value;
        break;
      }
    }
  }

  @Override
  public String toString() {
    return "Cblof [time=" + time + ", kc=" + kc + ", taxi=" + taxi + ", sf=" + sf + "]";
  }


}
