package com.weduoo.mapreduce.flowsum_partitioner;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 自定义分区的Partitioner
 * @author weduoo
 *
 */
public class FlowSumPartitioner {
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	//Map函数
	public static class FlowSumPartitionerMapper 
					extends Mapper<LongWritable, Text, Text, FlowBean>{
		Text k  = new Text();
		FlowBean v = new FlowBean();
		@Override
		protected void map(LongWritable key, Text value, Context context)
								throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");
			String  phone = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);
			k.set(phone);
			v.set(phone, upFlow, dFlow);
			context.write(k, v);
		}
	}
	//Reduce函数
	public static class FlowSumPartitionerReducer 
						extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			int upCount = 0;
			int dCount = 0;
			for (FlowBean flowBean : values) {
				upCount += flowBean.getUpFlow();
				dCount += flowBean.getDownFlow();
			}
			FlowBean sumBean = new FlowBean();
			sumBean.set(key.toString(), upCount, dCount);
			context.write(key, sumBean);
		}
	}
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSumPartitioner.class);
		
		job.setMapperClass(FlowSumPartitionerMapper.class);
		job.setReducerClass(FlowSumPartitionerReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//设置shuffle中的分区组件,使用我们自定义的分区组件
		job.setPartitionerClass(ProvincePartitioner.class);
		job.setNumReduceTasks(5);
		
		//设置输入输出路径，当输出路径存在时删除
		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/flow/input/"));
		Path out = new Path(LOCAL+"/flow/partitionerout/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out, true);
		}
		FileOutputFormat.setOutputPath(job,out);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
