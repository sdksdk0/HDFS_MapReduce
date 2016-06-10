package cn.tf.flow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean  implements WritableComparable<FlowBean>{
	
	private long upFlow;
	private long dFlow;
	private long sumFlow;
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getdFlow() {
		return dFlow;
	}
	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	public FlowBean(long upFlow, long dFlow) {
		super();
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow+dFlow;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow=in.readLong();
		dFlow=in.readLong();
		sumFlow=in.readLong();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dFlow);
		out.writeLong(sumFlow);
	}
	public FlowBean() {
		super();
	}

	@Override
	public String toString() {
		 
		return  upFlow + "\t" + dFlow + "\t" + sumFlow;
	}
	@Override
	public int compareTo(FlowBean o) {
		
		return this.sumFlow>o.getSumFlow() ? -1:1;
	}
	
	

}
