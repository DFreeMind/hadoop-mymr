package com.weduoo.mapreduce.top;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean>{

	private Text itemid;
	private DoubleWritable amount;
	
	public OrderBean(){
		
	}
	
	public OrderBean(Text itemid, DoubleWritable amount){
		set(itemid, amount);
	}
	
	public void set(Text itemid, DoubleWritable amount){
		this.itemid = itemid;
		this.amount = amount;
	}
	
	public Text getItemid() {
		return itemid;
	}

	public void setItemid(Text itemid) {
		this.itemid = itemid;
	}

	public DoubleWritable getAmount() {
		return amount;
	}

	public void setAmount(DoubleWritable amount) {
		this.amount = amount;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(itemid.toString());
		out.writeDouble(amount.get());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String readUTF = in.readUTF();
		double readDouble = in.readDouble();
		
		this.itemid = new Text(readUTF);
		this.amount = new DoubleWritable(readDouble);
		
	}

	@Override
	public int compareTo(OrderBean o) {
		//对订单ID相同的进行比较
		int num = this.itemid.compareTo(o.getItemid());
		if(num == 0){
			//默认是按照升序排列，返回负数之后，当前数值大的话
			//会排在前面，也就是实现降序排列
			return -this.amount.compareTo(o.getAmount());
		}
		return num;
	}
	@Override
	public boolean equals(Object obj) {
		OrderBean bean = (OrderBean) obj;

		return bean.getItemid().equals(this.itemid);
	}
	public String toString(){
		return itemid.toString() + "\t" + amount.get();
	}
}
