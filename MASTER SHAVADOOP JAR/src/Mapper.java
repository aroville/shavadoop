import java.io.PrintWriter;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author aroville, rvignes
 * 
 * This class is a delegate of the main thread. It receives the splitted file
 * and a list of SSH-available hosts, and divides the lines among the hosts.
 * It is also responsible for error handling: if a host fails for any reason,
 * its task must be started again in order not to lose 
 */
public class Mapper {

	static final String W = "/cal/homes/aroville/";
	static final int MAX_WORK_LOAD = 100;
	static final int STEP = 10;

	List<String> hosts;
	List<String> lines;
	List<MapThread> threads;
	Map<String, ArrayList<Integer>> keyUMx;

	/**
	 */
	Mapper(List<String> hosts, List<String> lines) {
		this.hosts = hosts;
		this.lines = lines;
		threads = new ArrayList<MapThread>();
		keyUMx = Collections.synchronizedMap(new HashMap<String, ArrayList<Integer>>());
	}

	/**
	 * 
	 */
	void distribute() {
		System.out.println("Start mapping");

		int nbHosts = hosts.size();
		String host;

		try {
			for (int i = 0; i < lines.size(); i += STEP) {
				while (threads.size() >= MAX_WORK_LOAD) {
					Thread.sleep(200);
				}

				if (writeLine(i)) {
					host = hosts.get(i % nbHosts);
					queue(new MapThread(host, i, this));
				}
			}

			while (threads.size() > 0) {
				System.out.println("Threads to finish = " + threads.size());
				Thread.sleep(400);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	synchronized void queue(MapThread t) {
		threads.add(t);
		t.start();
	}

	synchronized void dequeue(MapThread t) {
		threads.remove(t);
		storeKeyUMx(t);
	}


	/**
	 * Retrieve the collected keys and the associated file
	 * @param t
	 */
	private void storeKeyUMx(MapThread t) {
		for (String key: t.getKeys()) {
			if (key.length() == 0)
				continue;

			if (!keyUMx.containsKey(key))
				keyUMx.put(key, new ArrayList<Integer>());
			keyUMx.get(key).add(t.getIdx());
		}
	}

	public static String sansAccents(String input) { 
	    String s = Normalizer
	            .normalize(input, Normalizer.Form.NFD)
	            .replaceAll("[^\\p{ASCII}]", "");
	    
	    System.out.println(s);
	    return s;
	}

	private boolean writeLine(Integer i) {
		try {
			String s = lines.get(i);
			for (int k = i+1; k < Math.min(lines.size(), i + STEP); k++) {
				s += " " + lines.get(k);
			}
			
			s = sansAccents(s.trim());
			if (s.isEmpty())
				return false;

			PrintWriter writer = new PrintWriter(W + "Splits/S" + i, "UTF-8");
			writer.print(s);
			writer.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	// GETTER
	Map<String, ArrayList<Integer>> getKeyUMx() {
		return keyUMx;
	}

	void retry(MapThread t) {
		System.out.println("Retrying for idx = " + t.getIdx());
		threads.remove(t);

		try {
			Integer tIndex = ThreadLocalRandom.current().nextInt(0, hosts.size());

			while (threads.size() >= MAX_WORK_LOAD) {
				Thread.sleep(200);
			}
			queue(new MapThread(hosts.get(tIndex), t.getIdx(), this));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

}
