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

//cn.itcast.bigdata.mr.flowcount.FlowCount
public class FlowCount {

	public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
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
				} else {
					return;
				}
			} catch (Exception e) {

			}

		}

	}

	public static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

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

		job.setJarByClass(FlowCount.class);

		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.setInputPaths(job, new Path("/flow/data"));
		FileOutputFormat.setOutputPath(job, new Path("/flow/output"));

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}

}
