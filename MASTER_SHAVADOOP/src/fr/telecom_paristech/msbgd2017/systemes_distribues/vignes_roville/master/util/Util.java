package fr.telecom_paristech.msbgd2017.systemes_distribues.vignes_roville.master.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class Util {


	/**
	 * Read and return the lines in a file specified by its path
	 * @param fileUrl
	 * @return
	 * @throws IOException
	 */
	public static List<String> readFile(String fileUrl) throws IOException {
		return Files.readAllLines(Paths.get(fileUrl), Charset.forName("UTF-8"));
	}

	public static Map<String, Integer> sortByValue(Map<String, Integer> map) {
		List<Entry<String, Integer>> list = new LinkedList<Entry<String, Integer>>(map.entrySet());

		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return -(o1.getValue()).compareTo( o2.getValue());
			}
		});

		Map<String, Integer> result = new LinkedHashMap<String, Integer>();
		for (Entry<String, Integer> entry : list){
			result.put(entry.getKey(), entry.getValue());
		}

		return result;
	}

}
