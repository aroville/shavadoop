package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.slave;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Util {

	private static final Charset CHARSET = Charset.forName("ISO-8859-1");

	public static void createDirectory(String dirName) {
		File dir = new File(dirName);
		if (!dir.exists()) {
			try{
				dir.mkdir();
			} catch(SecurityException se){
				se.printStackTrace();
			}
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
