import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class Main {

	private static final String W = "/cal/homes/aroville/";
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	private static final int MODE = 0;
	private static final int S_INDEX = 1;
	private static final int KEY = 1;
	private static final int SM_INDEX = 2;



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


	/**
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

	public static List<String> readFile(String fileUrl) throws IOException {
		return Files.readAllLines(Paths.get(fileUrl), CHARSET);
	}

}