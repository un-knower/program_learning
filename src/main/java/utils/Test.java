package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    private static final String PHOTO_REGEX2 = "\\s*##Request_id:(.*?)##sid:(.*$)";
    public static void main(String args[]) {
        String s = "{\"nanos\":\"8095418225232254\",\"timestamp\":\"1532668062774\",\"body\":\"2018-07-27 13:07:42.720  INFO 4096 --- [l-2-thread-5381] c.xxxxxx.service.thrift.BidServiceImpl  : ##Request_id:osnxmbfoalzp2##sid:1\",\"host\":\"datastream34.lt.111.org\",\"priority\":\"INFO\",\"fields\":{\"HOSTNAME\":\"bbbbb24.dg.111.org\",\"_ds_position\":\"18155034155\",\"_ds_time_stamp\":\"1532668062721\",\"_ds_unique_id\":\"bbbbb24.dg.111.org-55290844660405197\",\"_ds_file_inode\":\"66846935\",\"_ds_target_dir\":\"/home/dsp/bj-recommend-dsp/bj-recommend-dsp/online-dsp/work/logs\",\"_ds_file_pattern\":\"app.log\",\"_ds_file_fingerprint\":\"8745f837616ef2ba385f3896566c393beec20ede\",\"_ds_agent_id\":\"46452\",\"TAG\":\"datacenter_rec_dsplog\",\"_ds_file_name\":\"app.log\"}}\n";
        JSONObject jsonObject = JSON.parseObject(s);
        String body = jsonObject.getString("body");
        System.out.println(body);

        String[] tokens = StringUtils.splitByWholeSeparatorPreserveAllTokens(body, " ");
        for(int i=0;i<tokens.length;i++) {
            System.out.println(i+":"+tokens[i]);
        }

        String requestIdSid = tokens[10];

        System.out.println("request_id_sid="+requestIdSid);
        Pattern pattern = Pattern.compile(PHOTO_REGEX2);
        Matcher matcher = pattern.matcher(requestIdSid);
        if (matcher.find()) {
            String group0 = matcher.group(0);
            String group1 = matcher.group(1);
            String group2 = matcher.group(2);
            System.out.println("0="+group0);
            System.out.println("1="+group1);
            System.out.println("2="+group2);
        }


    }
}
