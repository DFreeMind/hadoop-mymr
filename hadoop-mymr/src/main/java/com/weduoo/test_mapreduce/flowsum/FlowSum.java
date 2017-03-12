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
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	//map方法,输入是行的偏移量与一行数据，输出是手机号<Text>和FlowBean
	public static class FlowSumMapper 
			extends Mapper<LongWritable , Text , Text , FlowBean>{
		Text k = new Text();
		FlowBean v = new FlowBean();
		
		//将读到的一行数据进行切分，抽取业务数据
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			//抽取业务数据，手机号、上行流量、下行流量
			String phoneNum = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);
			//map输出将手机号作为K，FlowBean作为V，总流量在FlowBean的set
			//方法中计算并设值。此处将k、v在外层创建可以重复利用
			k.set(phoneNum);
			v.set(upFlow, dFlow);
			context.write(k, v);
		}
	}
	//Reduce函数，
	public static class FlowSumReducer 
		extends Reducer<Text, FlowBean, Text, FlowBean>{
		FlowBean v = new FlowBean();
		// reduce方法接收到的key是某一组<a手机号，bean><a手机号，bean><a手机号，bean>中的第一个手机号
		// reduce方法接收到的vlaues是这一组kv中的所有bean的一个迭代器
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long upFlowCount = 0;
			long dFlowCount = 0;
			//遍历计算上行与下行总流量
			for (FlowBean bean : values) {
				upFlowCount += bean.getUpFlow();
				dFlowCount += bean.getDownFlow();
			}
			//key已经是我们需要的可以直接使用，context的V还需要调用
			//FlowBean的set方法来计算并设值
			v.set(upFlowCount, dFlowCount);
			context.write(key, v);
		}
	}
	//设值驱动类
	public static void main(String[] args) throws IOException, 
						ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(FlowSum.class);
		
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		// 如果map阶段输出的数据类型跟最终输出的数据类型一致，就只要以下两行代码来指定
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		// 框架中默认的输入输出组件就是这两个，所以可以省略这两行代码
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path(LOCAL+"/flow/output"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
