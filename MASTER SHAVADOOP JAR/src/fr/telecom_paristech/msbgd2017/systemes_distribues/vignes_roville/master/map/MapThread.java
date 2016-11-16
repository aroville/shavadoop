package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**Class instantiated by the Mapper @see Mapper
 * giving an host, an index of split and the reference to the calling Mapper
 * @author aroville,rvignes
 *
 */


public class MapThread extends Thread {

	static final String JAR = "/cal/homes/aroville/workspace/MR_slave.jar";

	String host;
	Integer idx;
	Mapper mapper;
	List<String> keys;

/**
 * 
 * @param host
 * @param idx
 * @param mapper
 */
	MapThread(String host, Integer idx, Mapper mapper) {
		this.host = host;
		this.idx = idx;
		this.mapper = mapper;
	}

	/**
	 * Overriden run method of Thread 
	 * Starts the MapThread which remotely calls the Main of the Slave jar file.
	 * This call is done by a bash command line, which invoke the command java -jar with the path to the jar map option and 
	 * the index number of the Sx file on given host by ssh @see SSHUtils, Main of SLAVE SHAVADOOP JAR
	 */
	public void run() {
		keys = new ArrayList<String>();
		try {
			String[] args = new String[] {"ssh", host, "java -jar", JAR, "map", idx.toString()};

			ProcessResponse result = SSHUtils.execute(args);

			if (result.hasError()) {
				Thread.sleep(500);
				throw new Exception(result.getErrResponse());
			} else {
				keys = (List<String>) Arrays.asList(result.getStdResponse().split("\n"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			mapper.retry(this);
		} finally {
			mapper.dequeue(this);
		}
	}
	
	/**
	 * 
	 * @return MapThread's host
	 */
	String getHost() {
		return host;
	}

	/**
	 * 
	 * @return MapThread's list of keys 
	 */
	List<String> getKeys() {
		return keys;
	}

	/**
	 * 
	 * @return MapThread's split index
	 */
	Integer getIdx() {
		return idx;
	}
}
