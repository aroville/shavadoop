import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
	
/**
 * This Main class (from MASTER SHAVADOOP JAR) is responsible of :
 * 1) Retrieving available hosts from a list of hosts @see SSHUtils
 * 2) Reading the file on which we want to execute the wordcount @see Util
 * 3) Instanciating a Mapper which will split the read file on a fixed size of chunks	@see Mapper
 * 4) Instanciating a Reducer which will reduce by key (e.g a word) the unsorted map given by the Mapper @see Reducer
 * 5) Assembling all the results in a result file
 */

	private static final String W = "/cal/homes/aroville/";

	public static void main(String[] args) throws IOException {
		long startTime, timeSpent;
		Integer[] l = new Integer[] { 45, 125, 126, 127, 128, 129, 130, 133 };

		startTime = System.currentTimeMillis();
		List<String> hosts = SSHUtils.readHosts(l);
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on getting hosts: " + timeSpent);
		
		startTime = System.currentTimeMillis();
		List<String> lines = Util.readFile(args[0]);
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on splitting: " + timeSpent);

		startTime = System.currentTimeMillis();
		Mapper mapper = new Mapper(hosts, lines);
		mapper.split();
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on mapping: " + timeSpent);

		startTime = System.currentTimeMillis();
		Reducer reducer = new Reducer(hosts, mapper.getKeyUMx());
		reducer.reduce();
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on reducing: " + timeSpent);
		
		startTime = System.currentTimeMillis();
		Map<String, Integer> result = reducer.getReduceCount();
		PrintWriter writer = new PrintWriter(W + "RESULT_SHAVADOOP");
		for (Entry<String, Integer> key_count: result.entrySet()) {
			writer.println(key_count.getKey() + " " + key_count.getValue());
		}
		writer.close();
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on assembling: " + timeSpent);
	}

}
