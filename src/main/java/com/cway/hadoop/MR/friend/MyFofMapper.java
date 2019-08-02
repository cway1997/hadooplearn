package com.cway.hadoop.MR.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyFofMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	/**
	 *  tom hello hadoop cat
		world hadoop hello hive
		cat tom hive
		mr hive hello
		hive cat hadoop world hello mr
		hadoop tom hive world
		hello tom world hive mr
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] persons = value.toString().split(" ");
		String me = persons[0];
		for (int i = 1; i < persons.length; i++) {
			context.write(new Text(fof(me, persons[i])),new IntWritable(0));
			for (int j = i + 1; j < persons.length; j++) {
				context.write(new Text(fof(persons[j], persons[i])),new IntWritable(1));
			}
		}
	
	}
	
	private String fof(String name1,String name2){
		if(name1.compareTo(name2) > 0){
			return name1 + "_" + name2;
		}
		return name2 + "_" + name1;
	}
}
