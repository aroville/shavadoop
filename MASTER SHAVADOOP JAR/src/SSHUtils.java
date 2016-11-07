import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;


public class SSHUtils {
	
	/**
	 * Read the list of hosts retrieved by the python program
	 * @return
	 * @throws IOException
	 */
	static List<String> readHosts(Integer[] rooms) throws IOException {
		ArrayList<String> hosts = new ArrayList<String>();
		for (Integer room: rooms) {
//			getHosts(room.toString());
			hosts.addAll(Util.readFile("resources/ips_" + room));
		}
		return hosts;
	}


	/**
	 * Launch a python program to ping around the room and find the available machines
	 * @param room
	 */
	static void getHosts(String room) {
		try {
			Runtime.getRuntime().exec("python resources/get_hosts.py " + room).waitFor();
			System.out.println("Got hosts " + room);		
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
	

	
	public static ProcessResponse execute(String[] cmd) throws IOException {
		InputStream is = null;
		Process p = null;
		BufferedReader br = null;
		ProcessResponse output = null;
		Writer writerStd = new StringWriter();
		Writer writerErr = new StringWriter();
		ProcessBuilder pb = new ProcessBuilder(cmd);
		int n;
		try {
			p = pb.start();
			is = p.getInputStream();
			char[] buffer = new char[1024];
			br = new BufferedReader(new InputStreamReader(is));
			while ((n = br.read(buffer)) != -1) {
				writerStd.write(buffer, 0, n);
			}
			

			is = p.getErrorStream();
			buffer = new char[1024];
			br = new BufferedReader(new InputStreamReader(is));
			while ((n = br.read(buffer)) != -1) {
				writerErr.write(buffer, 0, n);
			}
			
			
			output = new ProcessResponse(writerStd.toString(), writerErr.toString());
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			writerStd.close();
			writerErr.close();
			if (br != null) 
				br.close();
			if (is != null) 
				is.close();
			if (p != null)
				p.destroy();
		}
		
		return output;
	}
	
	public static boolean canConnectSSH(String host) throws IOException {
		InputStream errIs = null;
		InputStreamReader isr = null;
		Process p = null;
		BufferedReader br = null;
		boolean output = false;
		try {
			ProcessBuilder pb = new ProcessBuilder("ssh", host, "echo 'pizza'");
			p = pb.start();
			Writer writer = new StringWriter();
			errIs = p.getErrorStream();
			char[] buffer = new char[1024];
			isr = new InputStreamReader(errIs);
			br = new BufferedReader(isr);
			int n;
			while ((n = br.read(buffer)) != -1) {
				writer.write(buffer, 0, n);
			}
			
			output = writer.toString().length() == 0;
			
			if (!output) {
				throw new Exception(writer.toString());
			}
			
			writer.close();
		} catch(Exception ex) {
			return false;
		} finally {
			if (br != null) 
				br.close();
			if (isr != null) 
				isr.close();
			if (errIs != null) 
				errIs.close();
			if (p != null)
				p.destroy();
		}
		
		return output;
	}

}
