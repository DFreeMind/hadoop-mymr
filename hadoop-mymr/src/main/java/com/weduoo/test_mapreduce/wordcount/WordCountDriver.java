package com.weduoo.test_mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver {

	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
//		System.setProperty("HADOOP_USER_NAME", "root");
//		conf.set("fs.defaultFS", "hdfs://192.168.11.101:9000");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "master");
		
		Job job = Job.getInstance(conf);
		
        //设置运行的主类
        job.setJarByClass(WordCountDriver.class);
        //设置Mapper和Reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        //设置输入输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置业务逻辑Reducer类的输出key和value的数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置输入输出处理方式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输入输出路径，setInputPaths方法可以设置多个输入路径
        FileInputFormat.setInputPaths(job, new Path(LOCAL+"/wordcount/input/"));
        FileOutputFormat.setOutputPath(job, new Path(LOCAL+"/wordcount/output/"));
//        FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
//        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output/"));

        //向yarn集群提价job
        //等待运行完成之后再退出，waitForCompletion的参数表示是否打印处理过程
        //job运行成功返回true
        System.exit(job.waitForCompletion(true)?0:1);

	}
}
