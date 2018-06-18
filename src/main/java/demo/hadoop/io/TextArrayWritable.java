package demo.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
/**
 今天写了一个map reduce程序，在map端输出是ArrayWritable类型的，
 可是到了reduce里报出了NoSuchMethodException: org.apache.Hadoop.io.ArrayWritable.<init>的问题。

在网上分别看了两篇文章：http://groups.google.com/group/nosql-databases/browse_thread/thread/b2434fdbf6e67e7d、
http://39382728.blog.163.com/blog/static/35360069201110995220372/，终于解决了问题。

具体的做法就是根据第二篇文章中写的一样，构造自定义子类TextArrayWritable，继承于ArrayWritable
 * @author qingjian
 *
 */
public class TextArrayWritable extends ArrayWritable {
  public TextArrayWritable() {
    super(Text.class);
  }

  public TextArrayWritable(Text[] strings) {
    super(Text.class, strings);
  }
}
