package com.cway.hadoop.MR.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text myKey = new Text();
    IntWritable myValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("key" + "=============");
        String[] words = StringUtils.split(value.toString(), ' ');
        for (String word : words) {
            myKey.set(word);
            context.write(myKey, myValue);
        }
    }
}
