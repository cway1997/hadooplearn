package com.cway.hadoop.MR.weather;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyTQReducer extends Reducer<MyTQ, DoubleWritable, MyTQ, DoubleWritable> {
	@Override
	protected void reduce(MyTQ key, Iterable<DoubleWritable> values,
			 Context context) throws IOException, InterruptedException {
		/**
		 * 	1949-10-01 10 null
			1949-10-02 13 null
			1949-10-01 14 null
			1949-10-09 19 null
			1949-10-10 20 null
		 */
		int flag = 0;
		int day = 0;
		for (DoubleWritable n: values) {
			if(flag == 0){
				context.write(key, null);
				day = key.getDay();
			}
			if(flag != 0 && day != key.getDay()){
				context.write(key, null);
				break;
			}
			flag ++;
		}
	}
}
