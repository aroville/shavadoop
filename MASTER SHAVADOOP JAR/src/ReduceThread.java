import java.util.ArrayList;
import java.util.Map.Entry;


public class ReduceThread extends Thread {

	private static final String JAR = "/cal/homes/aroville/workspace/MR_slave.jar";

	Integer idx;
	String key;
	String host;
	Reducer reducer;
	Integer count;
	ArrayList<Integer> UMx;


	public ReduceThread(Integer idx, String host, Entry<String, ArrayList<Integer>> e, Reducer reducer) {
		this.idx = idx;
		this.key = e.getKey();
		this.host = host;
		this.UMx = e.getValue();
		this.reducer = reducer;
	}

	public void run() {
		try {
			ProcessResponse resp = SSHUtils.execute(command());
			if (resp.hasError()) {
				throw new Exception(resp.getErrResponse());
			}
			
			count = Integer.parseInt(resp.getStdResponse().replaceAll("\n", ""));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			reducer.dequeue(this);
		}
	}
	
	private String[] command() {
		String sIdx = UMx.get(0).toString();
		for (int i = 1; i < UMx.size(); i++) {
			sIdx += " " + UMx.get(i);
		}
		
		return new String[] { "ssh", host, "java -jar", JAR, "reduce", key, idx.toString(), sIdx };
	}
	
	public String getKey() {
		return key;
	}
	
	public Integer getCount() {
		return count;
	}

}
