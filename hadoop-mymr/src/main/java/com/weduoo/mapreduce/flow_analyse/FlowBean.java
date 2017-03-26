package com.weduoo.mapreduce.flow_analyse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{

	private String url;
	private long upflow;
	
	public FlowBean(){}
	
	public void set(String url, long upflow){
		this.url = url;
		this.upflow = upflow;
	}
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public long getUpflow() {
		return upflow;
	}

	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(url);
		out.writeLong(upflow);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		url = in.readUTF();
		upflow = in.readLong();
		
	}

	@Override
	public int compareTo(FlowBean o) {
		return this.upflow > o.getUpflow()?-1:1;
	}
	@Override
	public String toString() {
		return this.url +"\t" +this.upflow;
	}
}
