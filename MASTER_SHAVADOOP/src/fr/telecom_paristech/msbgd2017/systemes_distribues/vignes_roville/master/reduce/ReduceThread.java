package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.reduce;

import java.util.ArrayList;
import java.util.Map.Entry;

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
	Integer count;
	Entry<String, ArrayList<Integer>> entry;


	public ReduceThread(Reducer reducer, String host) {
		this.host = host;
		this.reducer = reducer;
	}


	/**
	 * Overriden run method of Thread, calls the methods that runs the slave program via SSH.
	 */
	public void run() {
		runSlave();
	}


	/**
	 * Starts the ReduceThread which remotely calls the Main of the Slave jar file.
	 * This call is done by a bash command line, which invoke the command java -jar with the path to the jar reduce option and 
	 * the index number of the Sx file on given host by ssh @see SSHUtils, Main of SLAVE SHAVADOOP JAR
	 * Once the command is sent, it tries to read the count outputed by the Slave and stores it on the attribute count.
	 */
	public void runSlave() {
		while (reducer.getNextJob(this)) {
			try {
				ProcessResponse resp = SSHUtils.execute(command());
				if (resp.hasError()) {
					throw new Exception(resp.getErrResponse());
				}

				count = Integer.parseInt(resp.getStdResponse().replaceAll("\n", ""));
			} catch (Exception e) {
				e.printStackTrace();
				reducer.retry(this);
			}
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

		return new String[] { "ssh", host, "java -jar", Main.JAR, "reduce", key, idx.toString(), sIdx };
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

	void setIdx(Integer idx) {
		this.idx = idx;
	}

	void setEntry(Entry<String, ArrayList<Integer>> entry) {
		this.entry = entry;
	}


	/**
	 * Getter of UMx entries
	 * 
	 * @return Entry from UMx
	 */
	Entry<String, ArrayList<Integer>> getEntry() {
		return entry;
	}


}
