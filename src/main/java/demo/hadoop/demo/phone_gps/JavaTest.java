package demo.hadoop.demo.phone_gps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JavaTest {
	public static String jsonToKey(String key_json) {

		JSONObject jsonContent = JSON.parseObject(key_json);
		String dataContent = jsonContent.getString("data");
		String timeContent = jsonContent.getString("time");
		JSONObject dataJson = JSON.parseObject(dataContent);
		int mcc = Integer.parseInt(dataJson.getString("mcc"));
		int mnc = Integer.parseInt(dataJson.getString("mnc"));
		int lac = Integer.parseInt(dataJson.getString("lac"));
		int cid = Integer.parseInt(dataJson.getString("cid"));

		if (mnc >= 10000) {
			if (lac >= 0 && cid > 0) {
				return mnc + ", " + lac + ", " + cid+"\t"+timeContent;
			} else {
				return "";
			}
		}
		if (mcc < 0 || mnc < 0 || lac < 0 || cid <= 0 || cid > 0xFFFFFFE) {
			return "";
		}
		if (mcc == 460 && (mnc == 0 || mnc == 2 || mnc == 7) && lac > 0
				&& lac < 65534) {
			return "460, 0, " + lac + ", " + cid+"\t"+timeContent;
		}
		if (mcc == 460 && mnc == 1 && cid > 0 && lac > 0 && lac < 65534) {
			return "460, 1, " + lac + ", " + cid+"\t"+timeContent;
		}
		return "";

	}
	public static void main(String[] args) {
		String j = jsonToKey("");
		System.out.println(j);
	}
}
