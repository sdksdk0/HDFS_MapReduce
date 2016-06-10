package cn.tf.flow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartioner extends Partitioner<Text, FlowBean>{
	
	
private static HashMap<String, Integer> provinceMap = new HashMap<String, Integer>();
	
	static {
		
		provinceMap.put("135", 0);
		provinceMap.put("136", 1);
		provinceMap.put("137", 2);
		provinceMap.put("138", 3);
		
		
	}
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {

		String prefix = key.toString().substring(0, 3);
		Integer partNum = provinceMap.get(prefix);
		if(partNum == null) partNum=4;
		
		return partNum;
	}
	

}
