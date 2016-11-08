import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Mapper {
	/**
	 * Class instantiated by the main of MASTER SHAVADOOP JAR with the list of available hosts and all the lines from the input text
	 * 
	 * 
	 */

	static final String W = "/cal/homes/aroville/";
	static final int MAX_WORK_LOAD = 20;//Max MapThreads to start

	List<String> hosts;
	List<String> lines;
	List<MapThread> threads;
	Map<String, ArrayList<Integer>> keyUMx;

	/**
	 * The constructor of the Mapper wich initialise the attributes hosts with the list provided by the Main,
	 * the attribute lines with the list of lines provided by the Main.
	 * It also initialise the attribute threads with a new list of mappers @see MapThread class
	 * and the attribute keyUMx with a new Map of String,Arraylist
	 * @param hosts, lines
	 */
	Mapper(List<String> hosts, List<String> lines) {
		this.hosts = hosts;
		this.lines = lines;
		threads = new ArrayList<MapThread>();
		keyUMx = Collections.synchronizedMap(new HashMap<String, ArrayList<Integer>>());
	}

	/**
	 * This method distributes the data (one or several lines depending on the configuration of a split) and computation on the available Hosts.
	 * It proceeds by instanciating MapThreads, @see MapThread, giving the host on which we want the computation to be done
	 * and the index of the Sx file to give to the MapThread
	 * The distribution is done using a modulo between the number of splits and the number of available hosts
	 * @throws Exception
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

		System.out.println("Distribution over");
	}
	/**Enqueues a given MapThread and calls its start method 
	 * @param t
	 */
	synchronized void queue(MapThread t) {
		threads.add(t);
		t.start();
	}

	/**Dequeues a given MapThread and calls the method storeKeyUMx 
	 * @param t
	 */
	synchronized void dequeue(MapThread t)
	{
		threads.remove(t);
		storeKeyUMx(t);
	}

	/**
	 * Retrieve the collected keys from the SLAVE SHAVADOOP JAR output and the associated UMx file
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

/**
 * Writes the split i into a a Sx file
 * @param i
 * @return true if the line i is correctly written to the Sx file (file needed in input of the slave) 
 * or false if the line is empty or if any exception
 */
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

	/**
	 * Getter of keyUMx 
	 * 
	 * @return keyUMx
	 */
	Map<String, ArrayList<Integer>> getKeyUMx() {
		return keyUMx;
	}
	
	
	
	/**Restarts a given MapThread if its instanciation failed. 
	 * As we are network dependent (MapThread are started on other machines by SSH)
	 * we can encounter issues when trying to connect.
	 * @see MapThread
	 * @param t
	 */
	void retry(MapThread t) {
		System.out.println("Retrying for idx = " + t.getIdx());
		threads.remove(t);
		queue(new MapThread(t.getHost(), t.getIdx(), this));		
	}
	
	/**Cleans a given line by removing all non alphanumeric characters
	 * @return cleaned line s
	 * @param s 
	 */
	String clean(String s) {
		return s.replaceAll("[^a-zA-Z0-9]+"," ");
	}

}
