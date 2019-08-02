package com.cway.hadoop.MR.weather;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TQMR {
	public static void main(String[] args) throws Exception {
		//创建配置对象
		Configuration conf = new Configuration(true);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(TQMR.class);
		
		//输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		String output = args[1];
		Path path = new Path(output);
		FileSystem fs = path.getFileSystem(conf);
		if(fs.exists(path)){
			fs.delete(path,true);
		}
		FileOutputFormat.setOutputPath(job, path);
		
		//设置mapper类以及输出的k v类型
		job.setMapperClass(MyTQMapper.class);
		job.setMapOutputKeyClass(MyTQ.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		//设置reduce 类
		job.setReducerClass(MyTQReducer.class);
		
		//设置自定义的分组器
		job.setGroupingComparatorClass(MyTQGroupComparator.class);
		
		job.waitForCompletion(true);

	}
}
