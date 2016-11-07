import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MapThread extends Thread {

	static final String JAR = "/cal/homes/aroville/workspace/MR_slave.jar";

	String host;
	Integer idx;
	Mapper mapper;
	List<String> keys;


	MapThread(String host, Integer idx, Mapper mapper) {
		this.host = host;
		this.idx = idx;
		this.mapper = mapper;
	}

	public void run() {
		keys = new ArrayList<String>();
		try {
			String[] args = new String[] {"ssh", host, "java -jar", JAR, "map", idx.toString()};

			ProcessResponse result = SSHUtils.execute(args);

			if (result.hasError()) {
				Thread.sleep(500);
				throw new Exception(result.getErrResponse());
			} else {
				keys = (List<String>) Arrays.asList(result.getStdResponse().split("\n"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			mapper.retry(this);
		} finally {
			mapper.dequeue(this);
		}
	}
	
	String getHost() {
		return host;
	}

	List<String> getKeys() {
		return keys;
	}

	Integer getIdx() {
		return idx;
	}
}
