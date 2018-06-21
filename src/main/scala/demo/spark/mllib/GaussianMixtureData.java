package demo.spark.mllib;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class GaussianMixtureData {
	public static void main(String[] args) throws IOException {
	 
		BufferedReader br = new BufferedReader(new FileReader(new File("C:\\Users\\qingjian\\Desktop\\SampleTaxiFeature2")));
		String content = "";
		int sum = 0;
		while ((content = br.readLine()) != null) {
			String[] split = content.split(",");
			String firstOrderRatio = split[9];// 司机过去7天的订单中是乘客首单的比例 9
			String firstOrderFlag = split[10];// 本单是否为乘客首单 10
			String nonLocalRatio = split[11];// 司机过去7天的订单中乘客号码为异地（相对于发单地）的比例 11
			String nonLocalFlag = split[12];// 本单乘客号码是否为异地 12
			String groupSize = split[14];// 乘客所对应的团伙大小 14
			String groupOrderCount = split[15];// 司机过去7天与该团伙中所有人的成交单数 15
			String tradeIndex = split[16]; // 司机的频繁成交指数 16
			String label = split[20]; // 司机的频繁成交指数 16
			System.out.println(firstOrderRatio+","+firstOrderFlag+","+nonLocalRatio+","+nonLocalFlag+","+groupSize+","+groupOrderCount+","+tradeIndex+","+label);
		}
       br.close();
	}
}
