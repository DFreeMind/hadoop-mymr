package com.weduoo.test_mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	
	private long upFlow;
	private long dFlow;
	private long sumFlow;

	//反序列化框架在反序列化创建对象时调用无参构造函数
	public FlowBean() {
	}

	public void set(long upFlow, long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow + dFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	
	//在写出到磁盘时使用
	@Override
	public String toString() {
		return upFlow + "\t" + dFlow + "\t" + sumFlow;
	}

	//序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
		
	}
	
	//反序列化方法
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.dFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
	/**
	 * 此处有疑问，排序的机制是什么？
	 */
	@Override
	public int compareTo(FlowBean o) {
		// TODO Auto-generated method stub
		return  (int)(o.getSumFlow() - this.sumFlow);
	}

}
