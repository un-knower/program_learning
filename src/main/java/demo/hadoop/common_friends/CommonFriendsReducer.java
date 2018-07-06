package demo.hadoop.common_friends;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * 接收到数据
 * ("100,200", ["200,300,400,500", "100,300,400"])
 * ("100,300", ["200,400,500", "400"])
 * ("200,500", ["300,400", "100,300,400"])
 *
 * 那么"100,200"的共同friend是300 400
 * "100,300"的共同friend是400
 * "200,500"没有共同好友
 *
 */
public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Map<String, Integer> map =new HashMap<>();
        Iterator<Text> iterator = values.iterator();
        int numOfValues = 0;  // 统计value个数，value个数等于map的value值，则说明这个friend出现在每个数据中，是共同friend
        while (iterator.hasNext()) {
            String friends = iterator.next().toString();
            if (friends.equals("")) {
                context.write(key, new Text("[]"));
                return;
            }
            addFriends(map, friends);
            numOfValues ++;
        } // while

        List<String> commonFriends = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() == numOfValues) {
                commonFriends.add(entry.getKey());
            }
        }
        System.out.println(key+":"+commonFriends.toString());
        context.write(key, new Text(commonFriends.toString()));
    }

    private void addFriends(Map<String, Integer> map, String friendsStr) {
        String[] friends = StringUtils.splitByWholeSeparatorPreserveAllTokens(friendsStr, ",");
        for (String friend : friends) {
            Integer count = map.get(friend);
            if (count == null) {
                map.put(friend, 1);
            } else {
                map.put(friend, count + 1);
            }
        }
    }
}
