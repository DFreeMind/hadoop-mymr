package com.weduoo.mapreduce.friends;

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
import org.apache.hadoop.util.StringUtils;

public class FriendsStepOne {

	public final static String LOCAL = "/Users/weduoo/test/mapreduce";
	
	public static class FriendsStepOneMapper 
		extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//拿到数据A:B,C,D,F,E,O
			String line = value.toString();
			String[] host = line.split(":");
			String person = host[0];
			String[] friends = host[1].split(",");
			//遍历输出B,A C,A D,A F,A E,A O,A
			for (String friend : friends) {
				context.write(new Text(friend), new Text(person));
			}
		}
	}
	public static class FriendsStepOneReducer 
		extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text friends, Iterable<Text> persons, Context context)
				throws IOException, InterruptedException {
			//将<C,A> <C,B> <C,F> <C,M>…输出为
			//C A-B-F-M..
			String result = StringUtils.join("-", persons);
			context.write(friends, new Text(result));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(FriendsStepOneMapper.class);
		job.setReducerClass(FriendsStepOneReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path out = new Path(LOCAL+"/wordcount/friends-1/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/wordcount/friends/"));
		FileOutputFormat.setOutputPath(job, out);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
