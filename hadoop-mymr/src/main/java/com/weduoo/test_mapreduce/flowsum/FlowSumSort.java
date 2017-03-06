package com.weduoo.test_mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 自定义排序输出
 * @author weduoo
 *
 */
public class FlowSumSort {
	public static class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		FlowBean k = new FlowBean();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String phoneNum = fields[0];
			long upFlowCount = Long.parseLong(fields[1]);
			long dFlowCount = Long.parseLong(fields[2]);
			k.set(upFlowCount, dFlowCount);
			v.set(phoneNum);
			context.write(k, v);
		}
	}
	
	public static class FlowSumSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> phoneNum, Context context)
				throws IOException, InterruptedException {
			context.write(phoneNum.iterator().next(), bean);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSumSort.class);
		
		job.setMapperClass(FlowSumSortMapper.class);
		job.setReducerClass(FlowSumSortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path("/flow/output/"));
		FileOutputFormat.setOutputPath(job, new Path("/flow/sortout/"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
