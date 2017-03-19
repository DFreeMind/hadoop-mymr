package com.weduoo.mapreduce.flowsum_partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	private String phone;
	private long upFlow;
	private long downFlow;
	private long sumFlow;

	//反序列化框架在反序列化创建对象时调用无参构造函数
	public FlowBean() {
	}

	public void set(String phone ,long upFlow, long downFlow) {
		this.phone = phone;
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}
	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
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
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	//序列化方法
	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}
	
	//反序列化方法，按顺序读取数据和序列化中的顺序相同
	public void readFields(DataInput in) throws IOException {
		this.phone = in.readUTF();
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
	/**
	 * 比较排序
	 * 将当前的类与传入的类做比较，如果返回的是负数、零、正数
	 * 则表示当前类小于、等于、大于传入的类。
	 */
	public int compareTo(FlowBean o) {
		return  (int)(o.getSumFlow() - this.sumFlow);
	}

}
