import java.util.ArrayList;
import java.util.Map.Entry;


/**Class instantiated by the Reducer @see Reducer
 * giving an index of UM, an host, an entryset from the UM and the reference to the calling Reducer
 * @author aroville,rvignes
 *
 */

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
	/**
	 * Overriden run method of Thread 
	 * Starts the ReduceThread which remotely calls the Main of the Slave jar file.
	 * This call is done by a bash command line, which invoke the command java -jar with the path to the jar reduce option and 
	 * the index number of the Sx file on given host by ssh @see SSHUtils, Main of SLAVE SHAVADOOP JAR
	 * Once the command is sent, it tries to read the count outputed by the Slave and stores it on the attribute count.
	 * @throws Exception
	 */
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
	
	/**
	 * Build the command to be executed on remote host
	 * @return Build command line
	 */
	private String[] command() {
		String key = entry.getKey();
		ArrayList<Integer> UMx = entry.getValue();
		
		String sIdx = UMx.get(0).toString();
		for (int i = 1; i < UMx.size(); i++) {
			sIdx += " " + UMx.get(i);
		}
		
		return new String[] { "ssh", host, "java -jar", JAR, "reduce", key, idx.toString(), sIdx };
	}
	
	/**
	 * Getter of count
	 * @return ReduceThread's count
	 */
	public Integer getCount() {
		return count;
	}
	
	/**Getter of host
	 * 
	 * @return host
	 */
	String getHost() {
		return host;
	}

	/**
	 * Getter of UMx entryset's index
	 * @return idx
	 */
	Integer getIdx() {
		return idx;
	}
	
	/**Getter of UMx entries
	 * 
	 * @return  Entry from UMx
	 */
	Entry<String, ArrayList<Integer>> getEntry() {
		return entry;
	}
	

}
