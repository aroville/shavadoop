package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.map.Mapper;
import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.reduce.Reducer;
import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util.SSHUtils;
import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util.Util;

/**
 * 
 * @author aroville,rvignes
 * 
 * This software is a custom implementation of the MapReduce WordCount algorithm as described in the abstract from Jeff Dean.
 * It is composed of two separate pieces of code : the Master and the Slave.
 * The Master is responsible of reading an input file, for example Mayotte's Forest Code, splitting the file line by line
 * Distributing the splits (Sx files) on previously retrieved available hosts on the network.
 * the distribution is realised by starting on each host a MapThread giving a split as parameter @see Mapper,MapThread.
 * The MapThread also receive the code we want to be executed on the Split given as a Jar File with the map option @see SLAVE SHAVADOOP JAR.
 * In our case this code is a simple Word Count @see Main of SLAVE SHAVADOOP JAR.
 * The Slave outputs a file (UMx file) containing the result of the wordcount on its split.
 * The Main gets the keys and the name of the corresponding UMx File.
 * It then proceeds to the suffling stage by starting ReducerThreads giving the unsorted maps (UMx file) @see Reducer
 * and the reduce code described in the Slave Jar file to be executed (with the option reduce @see SLAVE SHAVADOOP JAR).
 * The ReducerThread outputs the ReduceMap both in RMx files and on the standard output. @see ReducerThread.
 * Finally, the Main in MASTER SHAVADOOP JAR gets back the RMx by the standard Output en assembles it on a result file.
 */
public class Main {

	static private long startTime;
	public static String W = "/cal/homes/aroville/workspace/MASTER_SHAVADOOP/";
	public static String JAR = W + "resources/slave.jar";

	/**
	 * This Main class (from MASTER_SHAVADOOP) is responsible for :
	 * 1) Retrieving available hosts from a list of hosts @see SSHUtils
	 * 2) Reading the file on which we want to execute the wordcount @see Util
	 * 3) Instanciating a Mapper which will split the read file on a fixed size of chunks	@see Mapper
	 * 4) Instanciating a Reducer which will reduce by key (e.g a word) the unsorted map given by the Mapper @see Reducer
	 * 5) Assembling all the results in a result file
	 */

	public static void main(String[] args) throws Exception {
		startTime = System.currentTimeMillis();
		
		List<String> hosts = SSHUtils.readHosts();
		printTimeSpentOn("getting hosts");

		String filename = args[0];
		List<String> lines = Util.readFile(filename);
		printTimeSpentOn("splitting");

		Mapper mapper = new Mapper(hosts, lines);
		mapper.distribute();
		printTimeSpentOn("mapping");

		Reducer reducer = new Reducer(hosts, mapper.getKeyUMx());
		reducer.reduce();
		printTimeSpentOn("reducing");

		writeResult(filename, reducer.getReduceCount());
		printTimeSpentOn("assembling");
	}

	
	/**
	 * Given a map of keys with their corresponding count, write the result of
	 * the wordcount in an output file.
	 * 
	 * @param filename The file where the result will appear
	 * @param result The key-count map resulting from the wordcount
	 */
	static private void writeResult(String filename, Map<String, Integer> result) {
		PrintWriter writer = null;
		try {
			int index = filename.lastIndexOf("/");
			String f = filename.substring(index + 1);
			writer = new PrintWriter(W + "out/RESULT_" + f);
			
			for (Entry<String, Integer> key_count: Util.sortByValue(result).entrySet()) {
				writer.println(key_count.getKey() + " " + key_count.getValue());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (writer != null)
				writer.close();
		}
	}


	/**
	 * Print the time ellapsed since we last reset "starTime".
	 * @param action The action that has been carried on
	 */
	static private void printTimeSpentOn(String action) {
		long timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on " + action + ":" + timeSpent);
		startTime = System.currentTimeMillis();
	}

}
