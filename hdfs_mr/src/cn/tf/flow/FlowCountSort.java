package cn.tf.flow;

import java.io.IOException;

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

public class FlowCountSort {

public static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line=value.toString();
			String[] fields=StringUtils.split(line,"\t");
			
			String phone=fields[0];
			long upSum=Long.parseLong(fields[1]);
			long dSum=Long.parseLong(fields[2]);
			
			FlowBean sumBean=new FlowBean(upSum,dSum);
			
			context.write(sumBean, new Text(phone));
		
		}	
}

	public static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		
		//进来的“一组”数据就是一个手机的流量bean和手机号
		@Override
		protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	
			context.write(values.iterator().next(), key);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowCountSort.class);

		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);

		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
	
	
	
	
	


}
