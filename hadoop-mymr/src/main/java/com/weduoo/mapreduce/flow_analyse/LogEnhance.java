package com.weduoo.mapreduce.flow_analyse;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogEnhance {
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	static class logEnhanceMapper 
			extends Mapper<LongWritable, Text, Text, NullWritable>{
		HashMap<String, String> knowledgeMap = new HashMap<String, String>();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			try {
				DBloader.loadDB(knowledgeMap);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Counter counter = context.getCounter("malformed","malformed_line");
			String line = value.toString();
			String[] fields = StringUtils.split(line,"\t");
			try {
				String url = fields[26];
				if(!url.startsWith("http")){
					counter.increment(1);
				}else{
					String content = knowledgeMap.get(url);
					String result = "";
					if(content == null){
						result = url +"\t" +"tocrawl\n";
					}else{
						result = line + "\t" + content + "\n";
					}
					context.write(new Text(result), NullWritable.get());
				}
				
			} catch (Exception e) {
				counter.increment(1);
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(LogEnhance.class);
		
		job.setMapperClass(logEnhanceMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//控制将不同的内容写入不同的路径
		job.setOutputFormatClass(LogOutputForamt.class);
		
		FileInputFormat.setInputPaths(job, new Path(LOCAL+"/flow/einput/"));
		//尽管我们用的是自定义outputformat，但是它是继承制fileoutputformat
		//在fileoutputformat中，必须输出一个success文件，所以在此还需要设置输出path
		Path out = new Path(LOCAL+"/flow/success/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileOutputFormat.setOutputPath(job, out);
		
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
