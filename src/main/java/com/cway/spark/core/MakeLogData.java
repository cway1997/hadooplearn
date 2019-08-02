package com.cway.spark.core;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * seesionId  userid time pageId channelId
 * @author eversec
 */


public class MakeLogData {
	public static void main(String[] args) throws Exception {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("d:/logdata")));
		Random random = new Random();
		int logNUm = 5000000;
		StringBuilder stringBuilder = new StringBuilder();
		List<String> channelList = Arrays.asList("spark","hdfs","mr","yarn","hive","scala","python");
		 
		for (int i = 0; i < logNUm; i++) {
			String sessionId = UUID.randomUUID().toString();
			int userId = random.nextInt(10000);
			int year = 2018;
			int month = random.nextInt(12) + 1;
			int day = random.nextInt(30) + 1;
			int hour = random.nextInt(24);
			int minute = random.nextInt(60);
			int second = random.nextInt(60);
			String dateTime = year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + second;
			SimpleDateFormat form = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");	
			long time = form.parse(dateTime).getTime();
			int pageId = random.nextInt(100);
			String channel = channelList.get(i % channelList.size() );
			stringBuilder.append(sessionId + "\t" + userId + "\t" + time + "\t" + pageId + "\t" + channel + "\n");
			bw.write(stringBuilder.toString());
			stringBuilder.delete(0, stringBuilder.length());
		}
		
		bw.flush();
		bw.close();
	}
}
