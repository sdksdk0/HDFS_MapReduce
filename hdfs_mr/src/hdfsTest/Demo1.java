package hdfsTest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class Demo1 {
	
	

	/*public static void main(String[] args) throws IOException {
		//download  from hdfs
		
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://ubuntu2:9000/");
		
		
		conf.set("dfs.blocksize", "64");
		 
		//to get a client of the hdfs system
		FileSystem fs = FileSystem.get(conf);
		
		fs.copyToLocalFile(new Path("hdfs://ubuntu2:9000/test.txt"), new Path("/home/admin1/Downloads/test1.txt"));
		
		fs.close();
	}*/
	
	@Test
	public void testUpload() throws IOException, InterruptedException, URISyntaxException{
		Configuration conf = new Configuration();
	
		 
		//to get a client of the hdfs system
		FileSystem fs = FileSystem.get(new URI("hdfs://ubuntu2:9000"),conf,"admin1");
		

		fs.copyFromLocalFile(new Path("/home/admin1/hadoop/eclipse.tar.gz"), new Path("/"));
		fs.close();
		
		
	}
	/*@Test
	public void rmove() throws IOException, InterruptedException, URISyntaxException{
	Configuration conf = new Configuration();

	FileSystem fs = FileSystem.get(new URI("hdfs://ubuntu2:9000"),conf,"admin1");
	
	fs.delete(new Path("/test.txt"));
	
	fs.close();
	}*/
	
	
	
	
	
}
