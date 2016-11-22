package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.slave;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;



/**This class contains all the pieces of code to be sent over the network (computation distribution) on available hosts.
 * It should be exported as a runnable JAR in order to be remotely invoked by a command line outputed by a MapThread or a ReduceThread
 * 
 * @author aroville, rvignes
 * @see Main of MASTER SHAVADOOP JAR, MapThread, ReduceThread 	
 */


public class Main {

	private static String W;

	private static final int MODE = 0;
	private static final int S_PATH = 1;
	private static final int S_INDEX = 2;
	private static final int KEY = 2;
	private static final int SM_INDEX = 3;

	private static ArrayList<String> PRONOMS;

	static {
		PRONOMS = new ArrayList<String>(Arrays.asList(new String[] {
				"je","tu","il","elle","nous","vous","ils","elles","le","la","l","lui","leur",
				"eux","celui","celle","ci","ceci","cela","mien","tien","sien","notre","votre",
				"mienne","tienne","sienne","miens","tiens","siens","votres","notres","leurs",
				"on","pas","ne","ni","dont","ou","certain","certaine","certains","certaines",
				"plusieurs","autre","quelqu","quelque","chose","qui","que","quoi","lequel",
				"laquelle","lesquels","lesquelles","auquel","auxquels","auxquelles","duquel",
				"desquelles","desquels", "de", "des", "aux", "au", "les", "par", "et", "du",
				"un", "une", "dans", "pour", "sur", "sous", "ce", "ces", "son", "cette", "qu",
				"est", "sont", "etre", "ainsi", "avec", "soit", "sa", "ca", "meme", "entre",
				"se", "lorsqu", "si", "ses", "ete", "ont", "ayant", "lors", "celles", "tous",
				"tout", "toute", "toutes", "comme", "avoir", "en"}));
	}



	/**Executes the map or reduce method considering the argument given to the Main(by the remote command line)
	 * 
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		W = args[S_PATH];

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
		List<String> linesToWrite = new ArrayList<String>();

		for (int i = SM_INDEX + 1; i < args.length; i++) {
			for (String line: Util.readFile(W + "/UnsortedMap/UM" + args[i])) {
				if (Pattern.matches("^" + args[KEY] + "\\s\\d*$", line)) {
					linesToWrite.add(line);
				}
			}
		}

		System.out.println(linesToWrite.size());
	}


	/**Computes the mapping given a split file (by its index) and writes the result on a UMx file
	 * 
	 * @param idx
	 * @throws IOException
	 */
	public static void map(Integer idx) throws IOException {
		List<String> lines = Util.readFile(W + "/Splits/S" + idx);
		if (lines.isEmpty())
			return;

		List<String> splitList = new ArrayList<String>(
				Arrays.asList(
						lines.get(0)
						.toLowerCase()
						.replaceAll("[^a-z0-9]+"," ")
						.trim()
						.split(" ")
						)
				);

		splitList.removeAll(PRONOMS);
		
		Util.createDirectory(W + "/UnsortedMap");
		PrintWriter writer = new PrintWriter(W + "/UnsortedMap/UM" + idx);
		for (String word: splitList) {
			if (word.length() > 1) {
				writer.println(word + " " + 1);
			}
		}
		writer.close();

		for (String word: new HashSet<String>(splitList)) {
			if (word.length() > 1) {
				System.out.println(word);
			}
		}
	}

}