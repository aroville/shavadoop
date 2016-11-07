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
 * Distributing the splits on previously retrieved available hosts on the network.
 * the distribution is realised by starting on each host a thread giving the 
 *  
 */
public class Main {

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
