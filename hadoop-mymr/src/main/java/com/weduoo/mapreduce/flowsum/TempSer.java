package com.weduoo.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TempSer implements WritableComparable<TempSer>{

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public int compareTo(TempSer o) {
		// TODO Auto-generated method stub
		return 0;
	}
	public static void main(String[] args) {
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < 8454333; i++) {
			str.append("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
		}
		System.err.println(str.hashCode());
	}
}
