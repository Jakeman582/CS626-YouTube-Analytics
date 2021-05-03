package Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

public class ChannelParser {
	
	// For fast lookup, we store each Channel into a Map
	private Map<String, Channel> channelMap;
	
	// Constructors
	public ChannelParser() {
		channelMap = new HashMap<String, Channel>();
	}
	
	public ChannelParser(Path path) {
		channelMap = new HashMap<String, Channel>();
		initialize(path);
	}
	
	public void initialize(Path path) {
		
		try {
			
			// Open the file for reading each channel line
			BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
			
			// Try reading from the file, and make sure to close
			try {
				String line = reader.readLine();
				while(line != null) {
					String[] channel = line.split("[\\t]");
					channelMap.put(channel[0], new Channel(
						channel[0],
						Double.parseDouble(channel[1]),
						Double.parseDouble(channel[2]),
						Double.parseDouble(channel[3])
					));
					line = reader.readLine();
				}
			} finally {
				reader.close();
			}
		} catch (IOException exception) {
			exception.printStackTrace();
		}
	}
	
	// Make sure we can get each part of a channel
	public DoubleWritable getAverageViews(String channelName) {
		return channelMap.get(channelName).getAverageViews();
	}
	
	public DoubleWritable getAverageComments(String channelName) {
		return channelMap.get(channelName).getAverageComments();
	}
	
	public DoubleWritable getAverageRatio(String channelName) {
		return channelMap.get(channelName).getAverageRatio();
	}
	
	/**/
	public Set<String> getKeys() {
		return channelMap.keySet();
	}

}
