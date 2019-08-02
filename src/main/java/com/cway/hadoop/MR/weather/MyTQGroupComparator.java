package com.cway.hadoop.MR.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyTQGroupComparator extends WritableComparator {
	
	//将反序列化(byte) 赋给 MyTQ对象
	public MyTQGroupComparator() {
		super(MyTQ.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//年   月 
		MyTQ t1 = (MyTQ)a;
		MyTQ t2 = (MyTQ)b;
		
		int yc = Integer.compare(t1.getYear(), t2.getYear());
		int mc = Integer.compare(t1.getMonth(), t2.getMonth());
		if(yc == 0){
			return mc;
		}else {
			return yc;
		}
	}
}
