package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;


public class SSHUtils {


	/**
	 * Read the list of hosts retrieved by the python program
	 * @return
	 * @throws IOException
	 */
	public static List<String> readHosts() throws Exception {
		String py = "python resources/get_hosts.py ";
		String rooms = "45 124 125 126 127 128 129 130 133";
		Runtime.getRuntime().exec(py + rooms).waitFor();
		return Util.readFile("resources/hosts");
	}


	/**Executes a given command line on the underlying Unix OS CLI.
	 * 
	 * @param cmd
	 * @return the OS answer to the inputed command line
	 * @throws IOException
	 */
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

	/**Tries to connect by ssh to a given host and tests it's availability by echoing 'pizza'
	 * 
	 * @param host
	 * @return host's availability
	 * @throws IOException
	 */

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
