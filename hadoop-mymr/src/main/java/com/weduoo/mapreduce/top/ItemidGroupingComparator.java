package com.weduoo.mapreduce.top;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * reduce端分组比较器
 * @author weduoo
 *
 */
public class ItemidGroupingComparator extends WritableComparator {

	protected ItemidGroupingComparator(){
		super(OrderBean.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean abean = (OrderBean)a;
		OrderBean bbean = (OrderBean)b;
		//将订单id相同的bean都视为相同，从而聚合为一组
		return abean.getItemid().compareTo(bbean.getItemid());
	}
}
