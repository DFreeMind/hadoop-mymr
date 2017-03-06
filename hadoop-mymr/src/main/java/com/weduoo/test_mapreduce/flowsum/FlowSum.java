package com.weduoo.test_mapreduce.flowsum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.weduoo.test_mapreduce.wordcount.WordCountDriver;
import com.weduoo.test_mapreduce.wordcount.WordCountMapper;
import com.weduoo.test_mapreduce.wordcount.WordCountReducer;

public class FlowSum {

	public static class FlowSumMapper extends Mapper<LongWritable , Text , Text , FlowBean>{
		Text k = new Text();
		FlowBean v = new FlowBean();
		
		//将读到的一行数据进行切分，抽取业务数据
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			//抽取业务数据
			String phoneNum = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);
			
			k.set(phoneNum);
			v.set(upFlow, dFlow);
			context.write(k, v);
		}
	}
	
	public static class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		FlowBean v = new FlowBean();
		// reduce方法接收到的key是某一组<a手机号，bean><a手机号，bean><a手机号，bean>中的第一个手机号
		// reduce方法接收到的vlaues是这一组kv中的所有bean的一个迭代器
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long upFlowCount = 0;
			long dFlowCount = 0;
			for (FlowBean bean : values) {
				upFlowCount += bean.getUpFlow();
				dFlowCount += bean.getdFlow();
			}	
			v.set(upFlowCount, dFlowCount);
			context.write(key, v);
		}
	}
	public static void main(String[] args) throws IOException, 
						ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		//job.setJar("/Users/weduoo/test/hadoop/wordcount.jar");
		job.setJarByClass(FlowSum.class);
		
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/weduoo/test/hadoop/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/weduoo/test/hadoop/flow/output"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
