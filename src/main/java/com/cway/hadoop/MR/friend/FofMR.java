package com.cway.hadoop.MR.friend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.cway.hadoop.MR.weather.MyTQGroupComparator;

public class FofMR {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(true);
		
		//创建job对象
		Job job = Job.getInstance(conf);
		
		//设置输入输出路径
		//输入输出路径
		FileInputFormat.setInputPaths(job, new Path("/data/fof/input/friend"));
		
		String output = "/data/fof/output";
		Path path = new Path(output);
		FileSystem fs = path.getFileSystem(conf);
		if(fs.exists(path)){
			fs.delete(path,true);
		}
		FileOutputFormat.setOutputPath(job, path);
		
		//设置mapper类以及输出的k v类型
		job.setMapperClass(MyFofMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//设置reduce 类
		job.setReducerClass(MyFofReducer.class);
		
		 job.waitForCompletion(true);
		
		
	}
}
