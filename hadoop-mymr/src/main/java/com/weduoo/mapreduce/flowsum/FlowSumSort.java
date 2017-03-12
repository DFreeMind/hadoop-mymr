package com.weduoo.mapreduce.flowsum;

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
 * 自定义排序输出，此处使用第一步已经输出的统计文件
 * MapReduce内部排序是根据key进行排序的
 * map输出是已经排好序的数据，reduce端拿到的数据是排序后的
 * 直接数据即可。因此需要在map端输出的时候在使用FLowBean作为
 * 输出的时候，map需要调用FlowBean中的compareTo方法进行比较，
 * 我们就在compareTo中设置排序的规则（从大到小、从小到大）
 * @author weduoo
 */
public class FlowSumSort {
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	//Map函数，此处将FlowBean作为输出K，手机号作为V
	public static class FlowSumSortMapper 
			extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		FlowBean k = new FlowBean();
		Text v = new Text();
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String phoneNum = fields[0];
			long upFlowCount = Long.parseLong(fields[1]);
			long dFlowCount = Long.parseLong(fields[2]);
			//将FlowBean作为key值输出，输出是会调用FlowBean的compareTo方法
			k.set(upFlowCount, dFlowCount);
			v.set(phoneNum);
			context.write(k, v);
		}
	}
	//Reducer函数
	public static class FlowSumSortReducer 
			extends Reducer<FlowBean, Text, Text, FlowBean>{
		
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> phoneNum, Context context)
				throws IOException, InterruptedException {
			//将手机号最为key输出
			context.write(phoneNum.iterator().next(), bean);
		}
	}
	
	public static void main(String[] args) throws 
				IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowSumSort.class);
		
		job.setMapperClass(FlowSumSortMapper.class);
		job.setReducerClass(FlowSumSortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		//第一步的输出作为输入
		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/flow/output/"));
		FileOutputFormat.setOutputPath(job, new Path(LOCAL+"/flow/sortout/"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
