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

public class TopN {

	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	static class TopNMapper 
			extends Mapper<LongWritable, Text, OrderBean, OrderBean>{
		OrderBean bean = new OrderBean();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			Double amount = new Double(fields[2]);
			bean.set(new Text(fields[0]), new DoubleWritable(amount));
			System.err.println(bean);
			//kv值相同
			context.write(bean, bean);
		}
	}
	static class TopNReducer 
		extends Reducer<OrderBean, OrderBean, NullWritable, OrderBean>{
		int topn = 1;
		int count = 0;
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//从环境中去的需要求的topn值
			Configuration conf = context.getConfiguration();
			topn = Integer.parseInt(conf.get("topn"));
		}
		@Override
		protected void reduce(OrderBean key, Iterable<OrderBean> values,Context context)
				throws IOException, InterruptedException {
			for (OrderBean bean : values) {
				if(count < topn){
					context.write(NullWritable.get(), bean);
					count++;
				}else {
					count = 0;
					return;
				}
			}
			count = 0;
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//设置全局环境变量，供MapReduce程序内部使用
		conf.set("topn", "2");
		Job job = Job.getInstance(conf);
		job.setJarByClass(TopN.class);
		
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setGroupingComparatorClass(ItemidGroupingComparator.class);
		job.setPartitionerClass(ItemidPartitioner.class);
		
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(OrderBean.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrderBean.class);
		
		Path out = new Path(LOCAL + "/top/topn/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, LOCAL + "/top/input/");
		FileOutputFormat.setOutputPath(job, out);
		
		job.setNumReduceTasks(1);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
