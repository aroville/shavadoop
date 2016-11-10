package com.telecom.slave.shavadoop

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;



/**This class contains all the pieces of code to be sent over the network (computation distribution) on available hosts.
 * It should be exported as a runnable JAR in order to be remotely invoked by a command line outputed by a MapThread or a ReduceThread
 * 
 * @author aroville, rvignes
 * @see Main of MASTER SHAVADOOP JAR, MapThread, ReduceThread 	
 */


public class Main {

	private static final String W = "/cal/homes/aroville/";
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	private static final int MODE = 0;
	private static final int S_INDEX = 1;
	private static final int KEY = 1;
	private static final int SM_INDEX = 2;



	/**Executes the map or reduce method considering the argument given to the Main(by the remote command line)
	 * 
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		String mode = args[MODE];
		if (mode.equals("map")) {
			map(Integer.parseInt(args[S_INDEX]));
		} else {
			reduce(args);
		}
	}


	/**Computes the reduce count from a given UMx file and outputs it on the standard output
	 * in order to be read by the reduceThread which executed the Slave JAR
	 * 
	 * @param idx
	 * @throws IOException 
	 */
	public static void reduce(String[] args) throws IOException {
		String key = args[KEY];
		String smx = args[SM_INDEX];

		List<String> lines;
		List<String> linesToWrite = new ArrayList<String>();
		
		for (int i = SM_INDEX+1; i < args.length; i++) {
			lines = readFile(W + "UnsortedMap/UM" + args[i]);
			for (String line: lines) {
				if (line.contains(key)) {
					linesToWrite.add(line);
				}
			}
		}

		PrintWriter writer2 = new PrintWriter(W + "ReduceMap/RM" + smx);
		writer2.println(key + " " + linesToWrite.size());
		writer2.close();
		
		System.out.println(linesToWrite.size());
	}


	/**Computes the mapping given a split file (by its index) and writes the result on a UMx file
	 * 
	 * @param idx
	 * @throws IOException
	 */
	public static void map(Integer idx) throws IOException {
		String input = readFile(W + "Splits/S" + idx).get(0);
		PrintWriter writer = new PrintWriter(W + "UnsortedMap/UM" + idx);

		HashSet<String> keys = new HashSet<String>();
		for (String word: input.split(" ")) {
			writer.println(word + " " + 1);
			keys.add(word);
		}
		writer.close();

		for (String key: keys) {
			System.out.println(key);
		}
	}

	/**read a given file and outputs all line as a list of lines
	 * 
	 * @param fileUrl
	 * @return all lines read from a file
	 * @throws IOException
	 */
	public static List<String> readFile(String fileUrl) throws IOException {
		return Files.readAllLines(Paths.get(fileUrl), CHARSET);
	}

}