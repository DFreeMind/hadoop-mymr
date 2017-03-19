package com.weduoo.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 
 * @author weduoo
 *
 */
public class IndexStepOne {
	public final static String LOCAL = "/Users/weduoo/test/mapreduce";
	//mapper
	public static class IndexStepOneMapper 
					extends Mapper<LongWritable, Text, Text, IntWritable>{
		Text k = new Text();
		IntWritable v = new IntWritable(1);
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//获取要处理的数据
			String line = value.toString();
			String[] words = line.split("\t");
			
			//获取文件名
			FileSplit inputSplit = (FileSplit) context.getInputSplit();
			String fileName = inputSplit.getPath().getName();
			
			//输出 key:word--fileName value:1
			for (String word : words) {
				k.set(word + "--" + fileName);
				context.write(k, v);
			}
		}
	}
	//Reducer
	public static class IndexStepOneReducer 
			extends Reducer<Text, IntWritable, Text, IntWritable>{
		IntWritable v = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable value : values) {
				count += value.get();
			}
			v.set(count);
			context.write(key, v);
		}
	}
	public static void main(String[] args) throws 
				IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		job.setMapperClass(IndexStepOneMapper.class);
		job.setCombinerClass(IndexStepOneReducer.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		Path out = new Path(LOCAL+"/wordcount/indexout-1/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/wordcount/index/"));
		FileOutputFormat.setOutputPath(job, out);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
