package com.weduoo.mapreduce.top;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 根据订单ID进行分组
 * @author weduoo
 *
 */
public class ItemidPartitioner 
	extends Partitioner<OrderBean, NullWritable>{
	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		return (key.getItemid().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
