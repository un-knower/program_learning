package demo.hadoop.join.replicatedjoin;

import java.io.*;

public interface DistributedCacheFileReader<K, V> extends Iterable<Pair<K, V>> {
  public void init(File f) throws IOException;
  public void close();
}
