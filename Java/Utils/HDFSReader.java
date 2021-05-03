package Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSReader {
	
	/**
	 * 
	 */
	public static ArrayList<String> readCacheFile(Path pathName) {
		
		// Make an ArrayList of the lines to return to the caller
		ArrayList<String> lines = new ArrayList<String>();
		
		try {
			
			// Store the input into a buffered reader so we can read each line
			BufferedReader reader = new BufferedReader(new FileReader(pathName.getName()));
			
			// Try reading from the file, and closing after finished
			try {
				String line = reader.readLine();
				while(line != null) {
					lines.add(line);
					line = reader.readLine();
				}
			} finally {
				// Make sure to close the reader
				reader.close();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lines;
		
	}
	
	/**
	 * 
	 */
	public static ArrayList<String> readFile(String fileName, Configuration config) {
		
		// Make an ArrayList to return to the caller
		ArrayList<String> lines = new ArrayList<String>();;
		
		try {
			
			// Try opening the file using the specified file system
			FileSystem fs = FileSystem.get(URI.create(fileName), config);
			FSDataInputStream in = fs.open(new Path(fileName));
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in, StandardCharsets.UTF_8)
			);
			
			// Try reading from the file, and closing after finished
			try {
				String line = reader.readLine();
				while(line != null) {
					lines.add(line);
					line = reader.readLine();
				}
				
			} finally {
				// Make sure to close the readers
				reader.close();
				in.close();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return lines;
		
	}

}
