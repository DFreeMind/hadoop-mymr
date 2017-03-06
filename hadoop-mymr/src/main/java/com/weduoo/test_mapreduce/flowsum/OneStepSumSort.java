package com.weduoo.test_mapreduce.flowsum;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OneStepSumSort {
	public static class OneStepSumSortMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		Text k = new Text();
		FlowBean v = new FlowBean();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 将读到的一行数据进行字段切分
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");

			// 抽取业务所需要的各字段
			String phoneNbr = fields[1];
			long upFlow = Long.parseLong(fields[fields.length - 3]);
			long dFlow = Long.parseLong(fields[fields.length - 2]);

			k.set(phoneNbr);
			v.set(upFlow, dFlow);

			context.write(k, v);

		}

	}

	public static class OneStepSumSortReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

		TreeMap<FlowBean, Text> treeMap = new TreeMap<FlowBean, Text>();
		

		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

			int upCount = 0;
			int dCount = 0;
			for (FlowBean bean : values) {

				upCount += bean.getUpFlow();
				dCount += bean.getdFlow();

			}
			//每次遍历都需要创建新的对象，这样才可以保证
			//treeMap中的数据是全部的数据，还不是之引用同一个对象
			//text也是一样，需要重新创建
			FlowBean sumBean = new FlowBean();
			sumBean.set(upCount, dCount);
			Text text = new Text(key.toString());
			treeMap.put(sumBean, text);
		}
		/**
		 * 利用cleanup最后将treeMap中缓存的结果输出
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Set<Entry<FlowBean, Text>> entrySet = treeMap.entrySet();
			for (Entry<FlowBean, Text> ent : entrySet) {

				context.write(ent.getValue(), ent.getKey());

			}

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(OneStepSumSort.class);

		// 告诉框架，我们的程序所用的mapper类和reducer类
		job.setMapperClass(OneStepSumSortMapper.class);
		job.setReducerClass(OneStepSumSortReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 告诉框架，我们的mapperreducer输出的数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 告诉框架，我们要处理的文件在哪个路径下
		FileInputFormat.setInputPaths(job, new Path("d:/flow/input/"));

		// 告诉框架，我们的处理结果要输出到哪里去
		FileOutputFormat.setOutputPath(job, new Path("d:/flow/sortout/"));

		boolean res = job.waitForCompletion(true);

		System.exit(res ? 0 : 1);

	

	}

}
