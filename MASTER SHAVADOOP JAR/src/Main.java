import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


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
