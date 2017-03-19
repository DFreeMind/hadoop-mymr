package com.weduoo.mapreduce.flowsum_partitioner;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner extends Partitioner<Text, FlowBean>{
	private static HashMap<String, Integer> provincMap = 
			new HashMap<String, Integer>();
	static {

		provincMap.put("138", 0);
		provincMap.put("139", 1);
		provincMap.put("136", 2);
		provincMap.put("137", 3);
		provincMap.put("135", 4);

	}
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		Integer code = provincMap.get(key.toString().substring(0, 3));
		if (code != null) {
			return code;
		}
		return 5;
	}

}
