package cn.tf.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN 输入数据kv对中的key的类型，默认情况下，key是一行文本的起始偏移量，Long VALUEIN
 * 输入数据kv的value的类型，默认情况下，value是一行文本的内容，String
 * 
 * 
 * KEYOUT 输出数据kv对中的key的类型，在我们这个wordcount逻辑中，输出的key是一个单词, String VALUEOUT
 * 输出数据kv对中的value的类型，在我们这个wordcount逻辑中，输出的value是一个1, Int
 * 
 * 但是在hadoop中，数据是要在各个节点之间进行网络传输的，需要进行序列化，而jdk的序列化机制太冗余，所以hadoop自定义了一套自己的序列化机制
 * 在hadoop中传输的数据就必须要实现hadoop自定义的序列化机制，所以，一些常用的基本数据类型在hadoop中都有相应的封装版，比如： Long
 * ————> LongWritable String ————> Text Integer ————> IntWritable
 * 
 * @author
 * 
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	/**
	 * mapper阶段的处理逻辑写在map方法中，mapreduce框架会有人来调用这个方法 默认情况下调用的规律是：每一行数据传递进来，就调用一次
	 * 
	 * 方法中的参数key，就是这一行的起始偏移量 方法中的参数value，就是这一行的内容
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 拿到一行的内容转换成String类型
		String line = value.toString();

		// 将这一行内容切分出单词
		String[] words = line.split(" ");

		// 遍历输出每一个单词和1
		for (String word : words) {

			context.write(new Text(word), new IntWritable(1));

		}

	}

}

