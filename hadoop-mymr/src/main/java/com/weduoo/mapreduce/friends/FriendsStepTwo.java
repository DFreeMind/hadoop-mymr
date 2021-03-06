package com.weduoo.mapreduce.friends;

import java.io.IOException;
import java.util.Arrays;

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

public class FriendsStepTwo {

	public final static String LOCAL = "/Users/weduoo/test/mapreduce";
	public static class FriendsStepTwoMapper 
		extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//拿到数据A     I-K-C-B-G-F-H-O-D
		String line = value.toString();
		String[] host = line.split("\t");
		String person = host[0];
		String[] friends = host[1].split("-");
		Arrays.sort(friends);
		//遍历输出B-C,A B-D,A ...
		for (int i = 0; i < friends.length-1; i++) {
			for (int j = i + 1; j < friends.length; j++) {
				context.write(new Text(friends[i] + "-" + friends[j]), new Text(person));
			}
			
		}
	}
}
	public static class FriendsStepTwoReducer 
		extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text person, Iterable<Text> friends, Context context)
				throws IOException, InterruptedException {
			//将<C,A> <C,B> <C,F> <C,M>…输出为
			//C A-B-F-M..
			String result = StringUtils.join(",", friends);
			context.write(person, new Text(result));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(FriendsStepTwoMapper.class);
		job.setReducerClass(FriendsStepTwoReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Path out = new Path(LOCAL+"/wordcount/friends-2/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/wordcount/friends-1/"));
		FileOutputFormat.setOutputPath(job, out);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
