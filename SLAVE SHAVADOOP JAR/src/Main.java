import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
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

	private static final String W = "/cal/homes/aroville/";
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	private static final int MODE = 0;
	private static final int S_INDEX = 1;
	private static final int KEY = 1;
	private static final int SM_INDEX = 2;
	
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
				if (Pattern.matches("^" + key + "\\s\\d*$", line)) {
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
		List<String> lines = readFile(W + "Splits/S" + idx);
		if (lines.isEmpty())
			return;
		
		String input = lines.get(0);
		input = input.toLowerCase();
		input = input.replaceAll("[^a-z0-9]+"," ");
		input = input.trim();
		if (input.length() == 0)
			return;
		
		String[] split = input.split(" ");
		List<String> splitList = new ArrayList<String>(Arrays.asList(split));
		splitList.removeAll(PRONOMS);
		if (splitList.isEmpty())
			return;
		
		// Optimize the timeframe where the file is open
		PrintWriter writer = new PrintWriter(W + "UnsortedMap/UM" + idx);
		for (String word: split) {
			if (word.length() > 1)
				writer.println(word + " " + 1);
		}
		writer.close();
		
		for (String word: new HashSet<String>(splitList)) {
			if (word.length() > 1)
				System.out.println(word);
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