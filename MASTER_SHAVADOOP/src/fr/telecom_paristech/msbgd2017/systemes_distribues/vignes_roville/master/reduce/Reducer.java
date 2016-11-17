package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.reduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;


public class Reducer {
/**
 * Class instantiated by the main of MASTER SHAVADOOP JAR with the list of available hosts and 
 * UMx outputed by the mapper
 */
	List<String> hosts;
	List<ReduceThread> threads;
	Map<String, ArrayList<Integer>> keyUMx;
	Map<String, Integer> reduceCount;
	Integer fileIdx = 0;

	
	/**
	 * 
	 * @param hosts
	 * @param keyUMx
	 */
	public Reducer(List<String> hosts, Map<String, ArrayList<Integer>> keyUMx) {
		this.hosts = hosts;
		this.keyUMx = Collections.synchronizedMap(keyUMx);
		threads = Collections.synchronizedList(new ArrayList<ReduceThread>());
		reduceCount = Collections.synchronizedMap(new HashMap<String, Integer>());
	}

	
	/**
	 * This method distributes the data (entry sets of UMx files) and computation on the available hosts.
	 * It proceeds by instanciating ReduceThreads, @see ReduceThread, giving the host on which we want the computation to be done,
	 * an entry of the UMx and it's index and a reference to the calling Reducer.
	 * The distribution is done using a modulo between the number of UMx and the number of available hosts
	 * @throws InterruptedException 
	 * @throws Exception
	 * 
	 */
	public void reduce() throws InterruptedException {
		System.out.println("Start reduce");

		for (String host: hosts) {
			threads.add(new ReduceThread(this, host));
		}
		
		while (!keyUMx.isEmpty()) {
			Thread.sleep(1000);
		}

		int nbHosts = hosts.size();
		String host;

		try {
			int i=0;
			for (Entry<String, ArrayList<Integer>> e: keyUMx.entrySet()) {
				host = hosts.get(i % nbHosts);
				queue(new ReduceThread(i++, host, e, this));
			}
			
			while (threads.size() > 0) {
				System.out.println("Threads to finish = " + threads.size());
				Thread.sleep(1000);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		System.out.println("Reduce over");
	}
	

	/**Enqueues a given ReduceThread and calls its start method 
	 * @param t
	 * @throws InterruptedException 
	 */
//	public synchronized void queue(ReduceThread t) throws InterruptedException {
//		while (threads.size() >= hosts.size()) {
//			Thread.sleep(100);
//		}
//		
//		threads.add(t);
//		t.start();
//	}
	

	/**Dequeues a given ReduceThread and calls the method storeReduceCount 
	 * @param t
	 */
	public synchronized void dequeue(ReduceThread t) {
		threads.remove(t);
		storeReduceCount(t);
	}
	
	
	/**
	 * Insert a key (e.g a word) and it's count onto the Reducer's reduceCount
	 * @param t
	 */
	public void storeReduceCount(ReduceThread t) {
		Integer count = t.getCount();
		
		if (count != null)
			reduceCount.put(t.getEntry().getKey(), count);
	}
	
	
	/**Getter of Reducer's reduceCount
	 * @return reduceCount
	 */
	public Map<String, Integer> getReduceCount() {
		return reduceCount;
	}
	
	
	public synchronized boolean getNextJob(ReduceThread t) {
		if (keyUMx.isEmpty())
			return false;
		
		Entry<String, ArrayList<Integer>> e = keyUMx.entrySet().iterator().next();
		keyUMx.remove(e.getKey());
		t.setEntry(e);
		t.setIdx(fileIdx++);
		return true;
	}
	
	
	/**Restarts a given ReduceThread if its instanciation failed. 
	 * As we are network dependent (ReduceThreads are started on other machines by SSH)
	 * we can encounter issues when trying to connect.
	 * @see ReduceThread
	 * @param ReduceThread t
	 */
	public void retry(ReduceThread t) {
		System.out.println("Retrying for idx = " + t.getIdx() + "  key = " + t.getEntry().getKey());
		threads.remove(t);

		try {
			int nbHosts = hosts.size();
			Integer tIndex = ThreadLocalRandom.current().nextInt(0, nbHosts);
			queue(new ReduceThread(t.getIdx(), hosts.get(tIndex), t.getEntry(), this));		
		} catch (InterruptedException e) {
			e.printStackTrace();
		}	
	}

}
