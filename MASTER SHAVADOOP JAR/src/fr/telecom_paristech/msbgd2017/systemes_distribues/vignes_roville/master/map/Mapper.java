package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.map;

import java.io.PrintWriter;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.Main;
import fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util.Util;

/**
 * @author aroville, rvignes
 * 
 * This class is a delegate of the main thread. It receives the splitted file
 * and a list of SSH-available hosts, and divides the lines among the hosts.
 * It is also responsible for error handling: if a host fails for any reason,
 * its task must be started again in order not to lose 
 */
public class Mapper {
	/* Class instantiated by the main of MASTER SHAVADOOP JAR with the list of available hosts 
	 * and all the lines from the input text */

	private static final int STEP = 200;

	private List<String> hosts;
	private List<String> lines;
	private List<MapThread> threads;
	private Map<String, ArrayList<Integer>> keyUMx;
	

	/**
	 * The constructor of the Mapper wich initialise the attributes hosts with the list provided by the Main,
	 * the attribute lines with the list of lines provided by the Main.
	 * It also initialise the attribute threads with a new list of mappers @see MapThread class
	 * and the attribute keyUMx with a new Map of String,Arraylist
	 * @param hosts, lines
	 */
	public Mapper(List<String> hosts, List<String> lines) {
		this.hosts = hosts;
		this.lines = lines;
		threads = new ArrayList<MapThread>();
		keyUMx = Collections.synchronizedMap(new HashMap<String, ArrayList<Integer>>());
		Util.createDirectory(Main.PATH + "Splits");
	}
	

	/**
	 * This method distributes the data (one or several lines depending on the configuration of a split) and computation on the available Hosts.
	 * It proceeds by instanciating MapThreads, @see MapThread, giving the host on which we want the computation to be done
	 * and the index of the Sx file to give to the MapThread
	 * The distribution is done using a modulo between the number of splits and the number of available hosts
	 * @throws Exception
	 *  
	 */
	public void distribute() {
		System.out.println("Start mapping");

		int nbHosts = hosts.size();
		String host;

		try {
			for (int i = 0; i < lines.size(); i += STEP) {
				while (threads.size() >= nbHosts) {
					Thread.sleep(200);
				}

				if (writeLine(i)) {
					host = hosts.get((i / STEP) % nbHosts);
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
	
	
	/**Enqueues a given MapThread and calls its start method 
	 * @param t
	 */
	public synchronized void queue(MapThread t) {
		threads.add(t);
		t.start();
	}
	

	/**Dequeues a given MapThread and calls the method storeKeyUMx 
	 * @param t
	 */
	public synchronized void dequeue(MapThread t)
	{
		threads.remove(t);
		storeKeyUMx(t);
	}

	
	/**
	 * Retrieve the collected keys from the SLAVE SHAVADOOP JAR output and the associated UMx file
	 * @param t
	 */
	public void storeKeyUMx(MapThread t) {
		for (String key: t.getKeys()) {
			if (key.length() == 0)
				continue;

			if (!keyUMx.containsKey(key))
				keyUMx.put(key, new ArrayList<Integer>());
			keyUMx.get(key).add(t.getIdx());
		}
	}

	
	/**
	 * Given a latin alphabet string, return it without special characters
	 * @param input The input string
	 * @return The clean string
	 */
	public static String sansAccents(String input) { 
		return Normalizer.normalize(input, Normalizer.Form.NFD)
				.replaceAll("[^\\p{ASCII}]", "");
	}

	
	/**
	 * Writes the split i into a a Sx file
	 * @param i
	 * @return true if the line i is correctly written to the Sx file (file needed in input of the slave) 
	 * or false if the line is empty or if any exception
	 */
	public boolean writeLine(Integer i) {
		try {
			String s = lines.get(i);
			for (int k = i+1; k < Math.min(lines.size(), i + STEP); k++) {
				s += " " + lines.get(k);
			}

			s = sansAccents(s.trim());
			if (s.isEmpty())
				return false;

			PrintWriter writer = new PrintWriter(Main.PATH + "/Splits/S" + i, "UTF-8");
			writer.print(s);
			writer.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	
	/**
	 * Getter of keyUMx
	 * @return keyUMx
	 */
	public Map<String, ArrayList<Integer>> getKeyUMx() {
		return keyUMx;
	}


	/**Restarts a given MapThread if its instanciation failed. 
	 * As we are network dependent (MapThread are started on other machines by SSH)
	 * we can encounter issues when trying to connect.
	 * @see MapThread
	 * @param t
	 */
	public void retry(MapThread t) {
		System.out.println("Retrying for idx = " + t.getIdx());
		threads.remove(t);

		try {
			Integer tIndex = ThreadLocalRandom.current().nextInt(0, hosts.size());

			while (threads.size() >= hosts.size()) {
				Thread.sleep(200);
			}
			
			queue(new MapThread(hosts.get(tIndex), t.getIdx(), this));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}

}
