import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
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
 * Distributing the splits on previously retrieved available hosts on the network.
 * the distribution is realised by starting on each host a MapThread ( @see MapThread) giving a split as parameter.
 * The MapThread also receive the code we want to be executed on the Split given as a Jar File @see SLAVE SHAVADOOP JAR.
 * In our case this code is a simple Word Count @see Main of SLAVE SHAVADOOP JAR.
 * The Slave outputs the 
 */
public class Main {

	private static final String W = "/cal/homes/aroville/";

	public static void main(String[] args) throws IOException {
		long startTime, timeSpent;
		Integer[] l = new Integer[] { 133 };

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
		mapper.distribute();
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on mapping: " + timeSpent);

		startTime = System.currentTimeMillis();
		Map<String, ArrayList<Integer>> keyUMx = mapper.getKeyUMx();
		System.out.println(keyUMx);
		Reducer reducer = new Reducer(hosts, keyUMx);
		reducer.reduce();
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on reducing: " + timeSpent);
		
		startTime = System.currentTimeMillis();
		Map<String, Integer> result = reducer.getReduceCount();
		PrintWriter writer = new PrintWriter(W + "RESULT_SHAVADOOP");
		for (Entry<String, Integer> key_count: Util.sortByValue(result).entrySet()) {
			writer.println(key_count.getKey() + " " + key_count.getValue());
		}
		writer.close();
		timeSpent = System.currentTimeMillis() - startTime;
		System.out.println("Time spent on assembling: " + timeSpent);
	}

}
