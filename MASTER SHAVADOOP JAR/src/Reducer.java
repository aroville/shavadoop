import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Reducer {

	List<String> hosts;
	List<ReduceThread> threads;
	Map<String, ArrayList<Integer>> keyUMx;
	Map<String, Integer> reduceCount;
	static final int MAX_WORK_LOAD = 20;

	public Reducer(List<String> hosts, Map<String, ArrayList<Integer>> keyUMx) {
		this.hosts = hosts;
		this.keyUMx = keyUMx;
		threads = Collections.synchronizedList(new ArrayList<ReduceThread>());
		reduceCount = Collections.synchronizedMap(new HashMap<String, Integer>());
	}

	void reduce() {
		System.out.println("Start reduce");
		long startTime, timeSpent;
		
		int nbHosts = hosts.size();
		String host;
		int i = 0;

		try {
			startTime = System.currentTimeMillis();
			for (Entry<String, ArrayList<Integer>> e: keyUMx.entrySet()) {
				while (threads.size() >= MAX_WORK_LOAD) {
					Thread.sleep(200);
				}
				
				host = hosts.get(i % nbHosts);
				queue(new ReduceThread(i++, host, e, this));
			}
			timeSpent = System.currentTimeMillis() - startTime;
			System.out.println("Time spent on shuffling: " + timeSpent);
			
			while (threads.size() > 0) {
				System.out.println("Threads to finish = " + threads.size());
				Thread.sleep(2000);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		System.out.println("Reduce over");
	}
	
	synchronized void queue(ReduceThread t) {
		threads.add(t);
		t.start();
	}

	synchronized void dequeue(ReduceThread t) {
		threads.remove(t);
		storeReduceCount(t);
	}
	
	void storeReduceCount(ReduceThread t) {
		reduceCount.put(t.getEntry().getKey(), t.getCount());
	}
	
	Map<String, Integer> getReduceCount() {
		return reduceCount;
	}
	
	void retry(ReduceThread t) {
		System.out.println("Retrying for idx = " + t.getIdx());
		threads.remove(t);
		
		queue(new ReduceThread(t.getIdx(), t.getHost(), t.getEntry(), this));		
	}

}
