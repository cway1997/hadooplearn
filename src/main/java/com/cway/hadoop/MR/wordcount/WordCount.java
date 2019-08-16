package com.cway.hadoop.MR.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 准备环境：
 * 1.window下的mapred-site.xml yarn-site.xml更新
 * 2.将mapred-site.xml yarn-site.xml 拷贝到工程下
 * 3.添加jar包依赖
 * 运行模式：
 * 1.local(在本地启动多个线程来模拟map task、reduce task 模拟执行) 测试环境
 * 将mapred-site.xml 中的mapreduce.framework.name 设置为local
 * 2.提交到集群中运行hadoop jar 生产环境
 * 代码->jar->client hadoop jar 提交
 * 3.在本机上提交任务到集群中运行
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);
//        System.setProperty("hadoop.home.dir", "G:\\java\\hadoop-2.7.4");
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
//        job.setJar("d:/hadooplearn.jar");
        // 设置输入路径
        FileInputFormat.setInputPaths(job, "/input/word_count");
        Path outputPath = new Path("/output/");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setReducerClass(WordCountReduce.class);

        job.setNumReduceTasks(2);

        job.waitForCompletion(true);

    }
}
