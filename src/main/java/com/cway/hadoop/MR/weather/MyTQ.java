package com.cway.hadoop.MR.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyTQ implements WritableComparable<MyTQ> {
	
	private int year;
	private int month;
	private int day;
	private double wd;
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public int getMonth() {
		return month;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public int getDay() {
		return day;
	}
	public void setDay(int day) {
		this.day = day;
	}
	public double getWd() {
		return wd;
	}
	public void setWd(double wd) {
		this.wd = wd;
	}
	
	@Override
	public String toString() {
		return year + "-" + month + "-" + day + ":" + wd ;
	}
	
	//序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		 out.writeInt(year);
		 out.writeInt(month);
		 out.writeInt(day);
		 out.writeDouble(wd);
	}
	
	//反序列化
	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readDouble();
	}
	@Override
	public int compareTo(MyTQ t) {
		int yc = this.year - t.getYear();
		if(yc == 0){
			int mc = this.month - t.getMonth();
			if(mc == 0){
				return -Double.compare(this.wd,t.getWd());
			}else{
				return -mc;
			}
		}else{
			return -yc;
		}
	}
	
}
