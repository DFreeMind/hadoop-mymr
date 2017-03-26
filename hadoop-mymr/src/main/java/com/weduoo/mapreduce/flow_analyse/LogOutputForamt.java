package com.weduoo.mapreduce.flow_analyse;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogOutputForamt extends FileOutputFormat<Text, NullWritable>{
	public static String LOCAL = "/Users/weduoo/test/mapreduce";
	/**
	 * map task 或者 reduce task在做最终输出时，它的工作步骤是：
	 * 1、通过用户conf中设置的outputformat实现类的getRecordWriter()方法
	 * 	获取到一个RecordWriter的具体实例对象
	 * 2、利用RecordWriter的具体实例对象调用其中的write(k,v,context)方法来将数据写出
	 */
	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		//不同数据输出的路径
		Path enPath = new Path(LOCAL+"/flow/enhanced/enhanced.txt");
		Path tocrawPath = new Path(LOCAL+"/flow/tocrawl/tocrawl.txt");
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		FSDataOutputStream enhanceOut = fs.create(enPath);
		FSDataOutputStream tocrawlOut = fs.create(tocrawPath);
		
		return new LogRecordWritable(enhanceOut,tocrawlOut);
	}
	//继承RecordWriter将不同的数据输入不同的路径
	public class LogRecordWritable extends RecordWriter<Text, NullWritable>{
		FSDataOutputStream enhanceOut;
		FSDataOutputStream tocrawlOut;
		
		public LogRecordWritable(FSDataOutputStream enhanceOut, FSDataOutputStream tocrawlOut) {
			this.enhanceOut = enhanceOut;
			this.tocrawlOut = tocrawlOut;
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			//写出数据判断数据是否是需要待爬数据
			if(key.toString().contains("tocrawl")){
				tocrawlOut.write(key.toString().getBytes());
			}else{
				enhanceOut.write(key.toString().getBytes());
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if(tocrawlOut != null){
				tocrawlOut.close();
			}
			if(enhanceOut != null){
				enhanceOut.close();
			}
		}
		
	}
}
