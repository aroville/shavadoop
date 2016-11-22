package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.reduce;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.Main;
import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util.ProcessResponse;
import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util.SSHUtils;


/**Class instantiated by the Reducer @see Reducer
 * giving an index of UM, an host, an entryset from the UM and the reference to the calling Reducer
 * @author aroville,rvignes
 *
 */

public class ReduceThread extends Thread {

	Integer idx;
	String host;
	Reducer reducer;
	Entry<Set<String>, Set<Integer>> entry;
	Map<String, Integer> result;


	public ReduceThread(Integer idx, String host, Entry<Set<String>, Set<Integer>> e, Reducer reducer) {
		this.idx = idx;
		this.host = host;
		this.reducer = reducer;
		this.entry = e;
		result = new HashMap<String, Integer>();
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
			
			
			String[] res = resp.getStdResponse().split("\n");
			for (String s: res) {
				String[] split = s.split(" ");
				result.put(split[0], Integer.parseInt(split[1]));
			}
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
		return new String[] { 
				"ssh", host, "java -jar", Main.JAR, "reduce", 
				Main.PATH, cmdLineKeysIdx(entry) };
	}


	/**
	 * Getter of result
	 * @return ReduceThread's result
	 */
	public Map<String, Integer> getResult() {
		return result;
	}


	/**
	 * Getter of host
	 * @return host
	 */
	public String getHost() {
		return host;
	}


	/**
	 * Getter of UMx entryset's index
	 * @return idx
	 */
	public Integer getIdx() {
		return idx;
	}


	/**
	 * Getter of UMx entries
	 * @return  Entry from UMx
	 */
	public Entry<Set<String>, Set<Integer>> getEntry() {
		return entry;
	}
	

	
	public String cmdLineKeysIdx(Entry<Set<String>, Set<Integer>> e) {
		String res = "";
		for (String k: e.getKey()) {
			res += k + " ";
		}
		res += "//=// ";
		for (Integer i: e.getValue()) {
			res += i.toString() + " ";
		}
		
		return res;
	}


}
