package com.weduoo.test_mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> KEYIN 是指框架读取到的数据的key的类型，
 * 在默认的InputFormat下，读到的key是一行文本的起始偏移量，所以key的类型是Long VALUEIN 是指框架读取到的数据的value的类型
 * ， 在默认的InputFormat下，读到的value是一行文本的内容，所以value的类型是String
 * 
 * KEYOUT 是指用户自定义逻辑方法返回的数据中key的类型，由用户业务逻辑决定，
 * 在此wordcount程序中，我们输出的key是单词，所以是String
 * 
 * VALUEOUT 是指用户自定义逻辑方法返回的数据中value的类型，由用户业务逻辑决定
 * 在此wordcount程序中，我们输出的value是单词的数量，所以是Integer
 * 
 * 但是，String ，Long等jdk中自带的数据类型，在序列化时，效率比较低，hadoop为了提高序列化效率，自定义了一套序列化框架
 * 所以，在hadoop的程序中，如果该数据需要进行序列化（写磁盘，或者网络传输），就一定要用实现了hadoop序列化框架的数据类型
 * 
 * Long ----> LongWritable S
 * tring ----> Text 
 * Integer ----> IntWritable 
 * Null   ----> NullWritable
 * @author
 * 
 */
public class WordCountMapper 
				extends Mapper<LongWritable, Text, Text, IntWritable>{
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] words = line.split(" ");
		for (String word : words) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
