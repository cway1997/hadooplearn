package com.cway.hadoop.MR.weather;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyTQMapper extends Mapper<LongWritable, Text, MyTQ, DoubleWritable> {

    MyTQ mkey = new MyTQ();
    DoubleWritable mval = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /**
         *  1949-10-01 14:21:02	34c
         1949-10-01 19:21:02	38c
         */
        String val = value.toString();
        String[] splited = val.split("\t");
        String time = splited[0];
        Double wd = Double.parseDouble(splited[1].split("c")[0]);
        String[] split = time.split(" ");
        String date = split[0];
        String[] split2 = date.split("-");
        int year = Integer.parseInt(split2[0]);
        int month = Integer.parseInt(split2[1]);
        int day = Integer.parseInt(split2[2]);

        mkey.setYear(year);
        mkey.setMonth(month);
        mkey.setDay(day);
        mkey.setWd(wd);

        context.write(mkey, mval);
    }
}
