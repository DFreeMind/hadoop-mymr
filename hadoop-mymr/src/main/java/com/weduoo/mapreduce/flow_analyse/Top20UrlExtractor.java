package com.weduoo.mapreduce.flow_analyse;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top20UrlExtractor {
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	static class Top20UrlMapper 
		extends Mapper<LongWritable, Text, Text, FlowBean>{
		
		Text k = new Text();
		FlowBean bean = new FlowBean();
		Counter malformedlines = null;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			malformedlines = context.getCounter("malformed","malformedlines");
			String line = value.toString();
			String[] fields = StringUtils.split(line, '\t');
			try {
				String url = fields[26];
				if(StringUtils.isNotBlank(url) && url.startsWith("http")){
					long upflow = Long.parseLong(fields[30]);
					k.set(url);
					bean.set(url, upflow);
					context.write(k, bean);
				}else{
					malformedlines.increment(1);
				}
			} catch (Exception e) {
				malformedlines.increment(1);
			}
		}
	}
	static class Top20UrlReducer 
			extends Reducer<Text, FlowBean, FlowBean, NullWritable>{
		private long globalCount = 0;
		private TreeMap<FlowBean, String> treeMap = new TreeMap<FlowBean, String>();
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context) 
				throws IOException, InterruptedException {
			long count = 0;
			for (FlowBean flowBean : values) {
				count += flowBean.getUpflow();
			}
			FlowBean countBean = new FlowBean();
			countBean.set(key.toString(), count);
			globalCount += count;
			treeMap.put(countBean, key.toString());
		}
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			Set<Entry<FlowBean, String>> entrySet = treeMap.entrySet();
			long tmpCount = 0;
			for (Entry<FlowBean, String> entry : entrySet) {
				if(tmpCount /globalCount < 0.8){
					tmpCount += entry.getKey().getUpflow();
					context.write(entry.getKey(), NullWritable.get());
				}else{
					return;
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(Top20UrlExtractor.class);
		job.setMapperClass(Top20UrlMapper.class);
		job.setReducerClass(Top20UrlReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		Path out = new Path(LOCAL + "/flow/top20");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, new Path(LOCAL + "/flow/einput"));
		FileOutputFormat.setOutputPath(job, out);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}