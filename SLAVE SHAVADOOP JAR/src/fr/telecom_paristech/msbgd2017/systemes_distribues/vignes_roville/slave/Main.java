package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.slave;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;



/**This class contains all the pieces of code to be sent over the network (computation distribution) on available hosts.
 * It should be exported as a runnable JAR in order to be remotely invoked by a command line outputed by a MapThread or a ReduceThread
 * 
 * @author aroville, rvignes
 * @see Main of MASTER SHAVADOOP JAR, MapThread, ReduceThread 	
 */


public class Main {

	private static String W;
	private static int argCount = 0;
	private static ArrayList<String> PRONOMS;

	private static final String STOP_FLAG = "//=//";

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
				"tout", "toute", "toutes", "comme", "avoir", "en", "jusqu"}));
	}



	/**Executes the map or reduce method considering the argument given to the Main(by the remote command line)
	 * 
	 * @param args
	 * @throws NumberFormatException 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws NumberFormatException, IOException {
		String mode = args[argCount++];
		W = args[argCount++];

		if (mode.equals("map")) {
			map(Integer.parseInt(args[argCount]));
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
		List<String> keys = getKeys(args);
		List<Integer> fileIndexes = getIdx(args);

		Map<String, Integer> res = new HashMap<String, Integer>();
		for (String k: keys) {
			res.put(k, 0);
		}

		for (Integer idx: fileIndexes) {
			for (String l: Util.readFile(W + "/UnsortedMap/UM" + idx)) {
				for (String k: keys) {
					if (Pattern.matches("^" + k + "\\s1$", l)) {
						res.put(k, res.get(k) + 1);
					}
				}
			}
		}

		for (Entry<String, Integer> e: res.entrySet()) {
			System.out.println(e.getKey() + " " + e.getValue());
		}
	}


	/**
	 * Retrieve the keys from the arguments
	 * @param args The arguments passed to the script
	 * @return An arrayList containing unique keys
	 */
	public static List<String> getKeys(String[] args) {
		List<String> keys = new ArrayList<String>();

		for (; argCount < args.length; argCount++) {
			String k = args[argCount];
			if (k.equals(STOP_FLAG)) {
				argCount++;
				break;
			}

			keys.add(k);
		}

		return keys;
	}


	/**
	 * Retrieve the keys from the arguments
	 * @param args The arguments passed to the script
	 * @return An arrayList containing unique keys
	 */
	public static List<Integer> getIdx(String[] args) {
		List<Integer> idx = new ArrayList<Integer>();

		for (; argCount < args.length; argCount++) {
			idx.add(Integer.parseInt(args[argCount]));
		}

		return idx;
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