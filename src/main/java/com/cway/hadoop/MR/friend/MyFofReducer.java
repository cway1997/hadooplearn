package com.cway.hadoop.MR.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyFofReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int flag = 0;
		int sum = 0;
		
		for (IntWritable elem : values) {
			if(elem.get() == 0){
				flag = 1;
				break;
			}
			sum ++;
		}
		
		if(flag == 0){
			context.write(key,new IntWritable(sum));
		}
		
	}
}
