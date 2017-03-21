package com.weduoo.mapreduce.join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapJoinDistributeCacheFile，添加缓存的map端连接
 * @author weduoo
 *
 */
public class MapJoinDCF {
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	public static class MapJoinDCFMapper 
			extends Mapper<LongWritable, Text, Text, NullWritable>{
		//用于读取缓存文件
		FileReader in = null;
		BufferedReader reader = null;
		
		//用于存放缓存数据product文件中的数据
		HashMap<String, String[]> table = new HashMap<String, String[]>();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			//加载产品数据文件
			in = new FileReader("pdts.txt");
			reader = new BufferedReader(in);
			String line = null;
			while(StringUtils.isNotBlank((line = reader.readLine()))){
				String[] split = line.split(",");
				String[] product = {split[0],split[1],split[3]};
				table.put(split[0], product);
			}
			IOUtils.closeStream(in);
			IOUtils.closeStream(reader);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//1001	20150710	P0001	2
			String line = value.toString();
			String[] orders = line.split("\t");
			String pdt_id = orders[2];
			String[] pdts = table.get(pdt_id); 
			String res = pdts[0] + "\t" + pdts[1]+"\t" + pdts[2] +  "\t"
						+ orders[0] + "\t" + orders[1] + "\t "+ orders[3];
			context.write(new Text(res), NullWritable.get());
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapJoinDCF.class);
		job.setMapperClass(MapJoinDCFMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		Path out = new Path(LOCAL + "/join/output/");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(out)){
			fs.delete(out);
		}
		FileInputFormat.setInputPaths(job, LOCAL + "/join/input/");
		FileOutputFormat.setOutputPath(job, out);
		//向集群中添加缓存文件
		job.addCacheFile(new URI(LOCAL+"/cache/pdts.txt"));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
