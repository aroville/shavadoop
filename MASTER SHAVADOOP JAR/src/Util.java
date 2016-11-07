import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;


public class Util {


	/**
	 * Read and return the lines in a file specified by its path
	 * @param fileUrl
	 * @return
	 * @throws IOException
	 */
	static List<String> readFile(String fileUrl) throws IOException {
		return Files.readAllLines(Paths.get(fileUrl), Charset.forName("ISO-8859-1"));
	}

}
