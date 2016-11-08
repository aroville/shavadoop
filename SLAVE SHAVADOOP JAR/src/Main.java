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


/**
 * 
 * @author arvignes
 *
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
				"desquelles","desquels", "de", "des", "aux", "au", "les", "par", "et"}));
	}



	/**
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


	/**
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

	/**
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

	public static List<String> readFile(String fileUrl) throws IOException {
		return Files.readAllLines(Paths.get(fileUrl), CHARSET);
	}

}