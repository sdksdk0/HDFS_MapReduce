package hdfsTest;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.Server;

public class ServerRunner {

	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Builder builder=new RPC.Builder(new Configuration());
		
		builder.setBindAddress("ubuntu2");
		builder.setPort(10000);
		
		builder.setProtocol(RPCService.class);
		builder.setInstance(new RPCServiceImpl());
		
		Server server=builder.build();
		server.start();

	}

}
