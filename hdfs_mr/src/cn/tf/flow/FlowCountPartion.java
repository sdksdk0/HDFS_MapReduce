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
/**
 * 流量汇总之后的结果需要按照省份输出到不同的结果文件中，需要解决两个问题： 1、如何让mr的最终结果产生多个文件： 原理：MR中的结果文件数量由reduce
 * task的数量绝对，是一一对应的 做法：在代码中指定reduce task的数量
 * 
 * 
 * 2、如何让手机号进入正确的文件 原理：让不同手机号数据发给正确的reduce task，就进入了正确的结果文件
 * 要自定义MR中的分区partition的机制（默认的机制是按照kv中k的hashcode%reducetask数）
 * 做法：自定义一个类来干预MR的分区策略——Partitioner的自定义实现类
 * 
 * @author
 * 
 */
public class FlowCountPartion {
	
	public static class FlowCountPartitionMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// 拿到这行的内容转成string
			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");
			try {
				if (fields.length > 3) {
					// 获得手机号及上下行流量字段值
					String phone = fields[1];
					long upFlow = Long.parseLong(fields[fields.length - 3]);
					long dFlow = Long.parseLong(fields[fields.length - 2]);

					// 输出这一行的处理结果,key为手机号，value为流量信息bean
					context.write(new Text(phone), new FlowBean(upFlow, dFlow));
					// Text(phone));
				} else {
					return;
				}
			} catch (Exception e) {

			}

		}

	}
	
	
	public static class FlowCountPartitionReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		
		
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

			long upSum = 0;
			long dSum = 0;

			for (FlowBean bean : values) {

				upSum += bean.getUpFlow();
				dSum += bean.getdFlow();
			}

			FlowBean resultBean = new FlowBean(upSum, dSum);
			context.write(key, resultBean);

		}
		
		
	}
	
	public static void main(String[] args) throws Exception {


		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowCountPartion.class);

		job.setMapperClass(FlowCountPartitionMapper.class);
		job.setReducerClass(FlowCountPartitionReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定自定义的partitioner
		job.setPartitionerClass(ProvincePartioner.class);
		
		job.setNumReduceTasks(5);
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	
	}
	

}
