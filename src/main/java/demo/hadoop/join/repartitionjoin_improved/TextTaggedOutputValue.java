package demo.hadoop.join.repartitionjoin_improved;

import org.apache.hadoop.io.*;

import java.io.*;

import demo.hadoop.join.repartitionjoin.impl.OutputValue;

public class TextTaggedOutputValue extends OutputValue {

  private Text data;

  public TextTaggedOutputValue() {
    this.data = new Text("");
  }

  public TextTaggedOutputValue(Text data) {
    this.data = data;
  }

  public Writable getData() {
    return data;
  }

  public void setData(Text data) {
    this.data = data;
  }

  public void write(DataOutput out) throws IOException {
    this.smaller.write(out);
    this.data.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    this.smaller.readFields(in);
    this.data.readFields(in);
  }
}
