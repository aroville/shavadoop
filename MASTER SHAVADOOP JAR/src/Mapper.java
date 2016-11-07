import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Mapper {

	static final String W = "/cal/homes/aroville/";
	static final int MAX_WORK_LOAD = 20;

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
	void split() {
		System.out.println("Start mapping");

		int nbHosts = hosts.size();
		String host;

		try {
			for (int i = 0; i < lines.size(); i++) {
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

		System.out.println("Splits over");
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


	private boolean writeLine(Integer i) {
		try {
			String s = clean(lines.get(i)).trim();
			if (s.length() == 0)
				throw new Exception("Empty string");
			
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
		queue(new MapThread(t.getHost(), t.getIdx(), this));		
	}
	
	String clean(String s) {
		return s.replaceAll("[^a-zA-Z0-9]+"," ");
	}

}
