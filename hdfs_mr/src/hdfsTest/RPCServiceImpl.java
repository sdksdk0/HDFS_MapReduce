package hdfsTest;

public class RPCServiceImpl  implements RPCService {



	@Override
	public String userLogin(String username, String password) {
		
		return username+"  logged in successfully!";
	}

}
