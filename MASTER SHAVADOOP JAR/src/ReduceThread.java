import java.util.ArrayList;
import java.util.Map.Entry;


public class ReduceThread extends Thread {

	private static final String JAR = "/cal/homes/aroville/workspace/MR_slave.jar";

	Integer idx;
	String host;
	Reducer reducer;
	Integer count;
	Entry<String, ArrayList<Integer>> entry;


	public ReduceThread(Integer idx, String host, Entry<String, ArrayList<Integer>> e, Reducer reducer) {
		this.idx = idx;
		this.host = host;
		this.reducer = reducer;
		this.entry = e;
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
			reducer.retry(this);
		} finally {
			reducer.dequeue(this);
		}
	}
	
	private String[] command() {
		String key = entry.getKey();
		ArrayList<Integer> UMx = entry.getValue();
		
		String sIdx = UMx.get(0).toString();
		for (int i = 1; i < UMx.size(); i++) {
			sIdx += " " + UMx.get(i);
		}
		
		return new String[] { "ssh", host, "java -jar", JAR, "reduce", key, idx.toString(), sIdx };
	}
	
	public Integer getCount() {
		return count;
	}
	
	String getHost() {
		return host;
	}

	Integer getIdx() {
		return idx;
	}
	
	Entry<String, ArrayList<Integer>> getEntry() {
		return entry;
	}
	

}
