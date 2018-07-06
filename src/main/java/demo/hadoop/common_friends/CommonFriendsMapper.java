package demo.hadoop.common_friends;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * map(100 200 300 400 500) =>
 * ("100,200","200,300,400,500")
 * ("100,300","200,300,400,500")
 * ("100,400","200,300,400,500")
 * ("100,500","200,300,400,500")
 *
 * key数字顺序按字典序排序
 */
public class CommonFriendsMapper extends Mapper<LongWritable,Text, Text,Text> {

    private static final Text mapperKey = new Text();
    private static final Text mapperValue = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(value.toString(), " ");
        String friends = getFriends(tokens);
        mapperValue.set(friends);
        String person = tokens[0];

        for (int i=1;i<tokens.length;i++) {
            String friend = tokens[i];
            String reducerKeyAsString = buileSortedKey(person, friend);
            mapperKey.set(reducerKeyAsString);
            System.out.println(person+"=>"+reducerKeyAsString);
            context.write(mapperKey, mapperValue);
        }
    }

    static String getFriends(String[] tokens) {
        /**
         * 100 200
         * 200 100 没有共同friend
         */
        if (tokens.length == 2) {
            return "";
        }
        StringBuffer builder = new StringBuffer();
        for (int i=1;i<tokens.length;i++) {
            builder.append(tokens[i]);
            if (i<tokens.length-1) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    /**
     * 100,200 和 200,100 统一为 100,200 一个key
     *
     *
     * @param person
     * @param friend
     * @return
     */
    static String buileSortedKey(String person, String friend) {
        long p = Long.parseLong(person);
        long f = Long.parseLong(friend);
        if (p < f) {
            return person+","+friend;
        } else {
            return friend+","+person;
        }
    }
}
