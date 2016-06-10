package cn.tf.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		
		Job wcjob=Job.getInstance(conf);
		
		wcjob.setJarByClass(JobRunner.class);
		
		//指定本job使用的mapper类
		wcjob.setMapperClass(WordCountMapper.class);
		wcjob.setReducerClass(WordCountReducer.class);
		
	//	wcjob.setCombinerClass(WordCountCombiner.class);
		
		
		wcjob.setMapOutputKeyClass(Text.class);
		wcjob.setMapOutputValueClass(IntWritable.class);
		
		wcjob.setOutputKeyClass(Text.class);
		wcjob.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(wcjob,new Path("/wc/data"));
		FileOutputFormat.setOutputPath(wcjob, new Path("/wc/output/"));

		boolean res=wcjob.waitForCompletion(true);
		System.exit(res?0:1);
	}

}
