package com.weduoo.mapreduce.top;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 输出每个订单中成交金额最大的记录
 * @author weduoo
 *
 */
public class TopOne {
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	static class TopOneMapper 
			extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
		OrderBean bean = new OrderBean();
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			Double amount = Double.parseDouble(fields[2]);
			bean.set(new Text(fields[0]), new DoubleWritable(amount));
			context.write(bean, NullWritable.get());
		}
	}
	static class TopOneReducer 
				extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
		// 在设置了groupingcomparator以后，这里收到的kv数据 就是： <1001 87.6>,null <1001
		// 76.5>,null ....
		// 此时，reduce方法中的参数key就是上述kv组中的第一个kv的key：<1001 87.6>
		// 要输出同一个item的所有订单中最大金额的那一个，就只要输出这个key
		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(TopOne.class);
		job.setMapperClass(TopOneMapper.class);
		job.setReducerClass(TopOneReducer.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		//指定shuffle是使用的Partitioner与GroupingComparator
		job.setGroupingComparatorClass(ItemidGroupingComparator.class);
		job.setPartitionerClass(ItemidPartitioner.class);
		
		job.setNumReduceTasks(1);
		
		Path out = new Path(LOCAL + "/top/topone/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, LOCAL + "/top/input/");
		FileOutputFormat.setOutputPath(job, out);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
