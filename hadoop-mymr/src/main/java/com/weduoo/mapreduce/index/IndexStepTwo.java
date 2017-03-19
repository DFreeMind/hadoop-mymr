package com.weduoo.mapreduce.index;

import java.io.IOException;

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


public class IndexStepTwo {

	public final static String LOCAL = "/Users/weduoo/test/mapreduce";
	public static class IndexStepTwoMapper 
		extends Mapper<LongWritable, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//将数据切分：hello--a.txt     3
			String line = value.toString();
			String[] fields = line.split("\t");
			String word_file = fields[0];
			String count = fields[1];
			String[] splits= word_file.split("--");
			String word = splits[0];
			String fileName = splits[1];
			
			k.set(word);
			v.set(fileName + "-->" +count);
			context.write(k, v);
		}
	}
	public static class IndexStepTwoReducer 
		extends Reducer<Text, Text, Text, Text>{
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer res = new StringBuffer();
			for (Text value : values) {
				res.append(value.toString()).append(" ");
			}
			v.set(res.toString());
			context.write(key, v);
		}
	}
	public static void main(String[] args) throws 
			IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepTwo.class);
		
		job.setMapperClass(IndexStepTwoMapper.class);
		job.setCombinerClass(IndexStepTwoReducer.class);
		job.setReducerClass(IndexStepTwoReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path out = new Path(LOCAL+"/wordcount/indexout-2/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/wordcount/indexout-1/"));
		FileOutputFormat.setOutputPath(job, out);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
