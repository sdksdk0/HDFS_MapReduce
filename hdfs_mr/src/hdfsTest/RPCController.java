package hdfsTest;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;


//
public class RPCController {

	public static void main(String[] args) throws IOException {
		RPCService serverceImpl=RPC.getProxy(RPCService.class,100,new InetSocketAddress("ubuntu2",10000),new Configuration());

		String result=serverceImpl.userLogin("aaa", "aaa");
		System.out.println(result);
	
	}

}
